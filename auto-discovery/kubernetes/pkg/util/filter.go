// SPDX-FileCopyrightText: the secureCodeBox authors
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	config "github.com/secureCodeBox/secureCodeBox/auto-discovery/kubernetes/pkg/config"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

var globalNamespaceCache *NamespaceCache

// NamespaceCache manages a local cache of enabled namespaces with real-time Kubernetes watch
type NamespaceCache struct {
	client     client.Client
	clientset  kubernetes.Interface
	cache      map[string]bool
	cacheMutex sync.RWMutex
	log        logr.Logger
	stopCh     chan struct{}
}

// NewNamespaceCache creates a new namespace cache
func NewNamespaceCache(client client.Client, log logr.Logger) *NamespaceCache {
	clientset, err := kubernetes.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		log.Error(err, "Failed to create clientset for namespace cache")
		return nil
	}

	return &NamespaceCache{
		client:    client,
		clientset: clientset,
		cache:     make(map[string]bool),
		log:       log,
		stopCh:    make(chan struct{}),
	}
}

// Start initializes the namespace cache and starts watching for changes
func (nc *NamespaceCache) Start(ctx context.Context) error {
	// Initial population of cache
	if err := nc.populateCache(ctx); err != nil {
		return fmt.Errorf("failed to populate initial namespace cache: %w", err)
	}

	// Start watching for namespace changes in a separate goroutine
	go nc.watchNamespaces(ctx)

	return nil
}

// Stop stops the namespace cache watcher
func (nc *NamespaceCache) Stop() {
	close(nc.stopCh)
}

// IsNamespaceEnabled checks if a namespace has auto-discovery enabled
func (nc *NamespaceCache) IsNamespaceEnabled(namespaceName string) bool {
	nc.cacheMutex.RLock()
	defer nc.cacheMutex.RUnlock()
	return nc.cache[namespaceName]
}

// populateCache loads all namespaces with the enabled annotation into the cache
func (nc *NamespaceCache) populateCache(ctx context.Context) error {
	// Use the direct clientset instead of the cached client to avoid dependency on manager cache
	namespaces, err := nc.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	nc.cacheMutex.Lock()
	defer nc.cacheMutex.Unlock()

	// Clear existing cache
	nc.cache = make(map[string]bool)

	// Populate cache with enabled namespaces
	for _, ns := range namespaces.Items {
		if val, ok := ns.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
			nc.cache[ns.Name] = true
			nc.log.V(8).Info("Namespace cached as enabled", "namespace", ns.Name)
		}
	}

	nc.log.Info("Populated namespace cache", "enabledNamespaces", len(nc.cache))
	return nil
}

// watchNamespaces uses Kubernetes watch API for real-time namespace updates
func (nc *NamespaceCache) watchNamespaces(ctx context.Context) {
	for {
		// Watch all namespace changes since we need to detect both:
		// 1. Namespaces getting the annotation added/removed
		// 2. Namespaces being created/deleted with the annotation
		// Field selectors don't support custom annotations, so we filter in code
		listOptions := metav1.ListOptions{
			Watch: true,
		}

		watcher, err := nc.clientset.CoreV1().Namespaces().Watch(ctx, listOptions)
		if err != nil {
			nc.log.Error(err, "Failed to create namespace watcher, retrying...")
			select {
			case <-ctx.Done():
				return
			case <-nc.stopCh:
				return
			default:
				continue
			}
		}

		// Process watch events
		func() {
			defer watcher.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-nc.stopCh:
					return
				case event, ok := <-watcher.ResultChan():
					if !ok {
						nc.log.Info("Namespace watcher channel closed, restarting...")
						return
					}

					namespace, ok := event.Object.(*corev1.Namespace)
					if !ok {
						nc.log.Error(fmt.Errorf("unexpected object type"), "Failed to cast to Namespace")
						continue
					}

					switch event.Type {
					case watch.Added, watch.Modified:
						nc.updateNamespace(namespace)
					case watch.Deleted:
						nc.removeNamespace(namespace.Name)
					default:
						nc.log.V(8).Info("Unhandled watch event type", "type", event.Type)
					}
				}
			}
		}()
	}
}

// updateNamespace updates the cache for a specific namespace
func (nc *NamespaceCache) updateNamespace(namespace *corev1.Namespace) {
	nc.cacheMutex.Lock()
	defer nc.cacheMutex.Unlock()

	enabled := false
	if val, ok := namespace.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
		enabled = true
	}

	wasEnabled := nc.cache[namespace.Name]

	if enabled {
		if !wasEnabled {
			nc.log.Info("Namespace enabled for auto-discovery", "namespace", namespace.Name)
		}
		nc.cache[namespace.Name] = true
	} else {
		if wasEnabled {
			nc.log.Info("Namespace disabled for auto-discovery", "namespace", namespace.Name)
		}
		delete(nc.cache, namespace.Name)
	}
}

// removeNamespace removes a namespace from the cache
func (nc *NamespaceCache) removeNamespace(namespaceName string) {
	nc.cacheMutex.Lock()
	defer nc.cacheMutex.Unlock()

	if nc.cache[namespaceName] {
		nc.log.Info("Removing deleted namespace from cache", "namespace", namespaceName)
	}
	delete(nc.cache, namespaceName)
}

func getNamespace(client client.Client, name string) (*corev1.Namespace, error) {
	namespace := corev1.Namespace{}
	err := client.Get(context.Background(), types.NamespacedName{Name: name}, &namespace)
	if err != nil {
		return nil, err
	}

	return &namespace, nil
}

func getNamespaceName(object client.Object) string {
	if object.GetNamespace() == "" {
		// The Object is not namespaced...
		return object.GetName()
	}

	return object.GetNamespace()
}

// InitializeNamespaceCache initializes the global namespace cache if needed
func InitializeNamespaceCache(ctx context.Context, client client.Client, log logr.Logger) error {
	if globalNamespaceCache == nil {
		globalNamespaceCache = NewNamespaceCache(client, log)
		if globalNamespaceCache == nil {
			return fmt.Errorf("failed to create namespace cache")
		}
		if err := globalNamespaceCache.Start(ctx); err != nil {
			return fmt.Errorf("failed to start namespace cache: %w", err)
		}
	}
	return nil
}

func GetPredicates(client client.Client, log logr.Logger, resourceInclusionMode config.ResourceInclusionMode) predicate.Predicate {
	log.Info("Setting up Predicate Filter", "resourceInclusionMode", resourceInclusionMode)

	switch resourceInclusionMode {
	case config.EnabledPerResource:
		return getPredicatesForEnabledPerResource(client, log)
	case config.All:
		return getPredicatesForScanAll(client, log)
	case config.EnabledPerNamespace:
		// Check if namespace cache is initialized
		if globalNamespaceCache == nil {
			log.Error(fmt.Errorf("namespace cache not initialized"), "Namespace cache must be initialized before using EnabledPerNamespace mode")
			// Fall back to old behavior if cache is not initialized
			return getPredicatesForEnabledPerNamespaceFallback(client, log)
		}
		return getPredicatesForEnabledPerNamespaceCached(client, log)
	}

	panic(fmt.Errorf("Invalid resourceInclusion.mode configured: '%s'. Check docs for supported modes.", resourceInclusionMode))
}

func getPredicatesForEnabledPerNamespaceFallback(client client.Client, log logr.Logger) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {

			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespace, err := getNamespace(client, getNamespaceName(event.Object))
			if err != nil {
				log.Error(err, "Failed to get Namespace")
			}

			if val, ok := namespace.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		DeleteFunc: func(event event.DeleteEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespace, err := getNamespace(client, getNamespaceName(event.Object))
			if err != nil {
				log.Error(err, "Failed to get Namespace")
			}

			if val, ok := namespace.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		UpdateFunc: func(event event.UpdateEvent) bool {
			if val, ok := event.ObjectNew.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespace, err := getNamespace(client, getNamespaceName(event.ObjectNew))
			if err != nil {
				log.Error(err, "Failed to get Namespace")
			}

			if val, ok := namespace.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		GenericFunc: func(event event.GenericEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespace, err := getNamespace(client, getNamespaceName(event.Object))
			if err != nil {
				log.Error(err, "Failed to get Namespace")
			}

			if val, ok := namespace.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
	}
}

func getPredicatesForEnabledPerNamespaceCached(client client.Client, log logr.Logger) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespaceName := getNamespaceName(event.Object)
			return globalNamespaceCache.IsNamespaceEnabled(namespaceName)
		},
		DeleteFunc: func(event event.DeleteEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespaceName := getNamespaceName(event.Object)
			return globalNamespaceCache.IsNamespaceEnabled(namespaceName)
		},
		UpdateFunc: func(event event.UpdateEvent) bool {
			if val, ok := event.ObjectNew.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespaceName := getNamespaceName(event.ObjectNew)
			return globalNamespaceCache.IsNamespaceEnabled(namespaceName)
		},
		GenericFunc: func(event event.GenericEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}

			namespaceName := getNamespaceName(event.Object)
			return globalNamespaceCache.IsNamespaceEnabled(namespaceName)
		},
	}
}

func getPredicatesForEnabledPerResource(_ client.Client, _ logr.Logger) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		DeleteFunc: func(event event.DeleteEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		UpdateFunc: func(event event.UpdateEvent) bool {
			if val, ok := event.ObjectNew.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
		GenericFunc: func(event event.GenericEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/enabled"]; ok && val == "true" {
				return true
			}
			return false
		},
	}
}

func getPredicatesForScanAll(_ client.Client, _ logr.Logger) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event event.CreateEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}
			return true
		},
		DeleteFunc: func(event event.DeleteEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}
			return true
		},
		UpdateFunc: func(event event.UpdateEvent) bool {
			if val, ok := event.ObjectNew.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}
			return true
		},
		GenericFunc: func(event event.GenericEvent) bool {
			if val, ok := event.Object.GetAnnotations()["auto-discovery.securecodebox.io/ignore"]; ok && val == "true" {
				return false
			}
			return true
		},
	}
}
