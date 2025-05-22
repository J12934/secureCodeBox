// SPDX-FileCopyrightText: the secureCodeBox authors
//
// SPDX-License-Identifier: Apache-2.0

package scancontrollers

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	batch "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	executionv1 "github.com/secureCodeBox/secureCodeBox/operator/apis/execution/v1"
	file_storage "github.com/secureCodeBox/secureCodeBox/operator/internal/file_storage"
)

// ScanReconciler reconciles a Scan object
type ScanReconciler struct {
	client.Client
	Log         logr.Logger
	Scheme      *runtime.Scheme
	FileStorage file_storage.FileStorage
}

var (
	ownerKey = ".metadata.controller"
	apiGVStr = executionv1.GroupVersion.String()
)

// Finalizer to delete related files in s3 when the scan gets deleted
// https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#finalizers
var s3StorageFinalizer = "s3.storage.securecodebox.io"

// +kubebuilder:rbac:groups=execution.securecodebox.io,resources=scans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=execution.securecodebox.io,resources=scans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=execution.securecodebox.io,resources=scantypes,verbs=get;list;watch
// +kubebuilder:rbac:groups=execution.securecodebox.io,resources=parsedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=execution.securecodebox.io,resources=scancompletionhooks,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// Permissions needed to create service accounts for lurker, parser and scanCompletionHooks

// Pod permission are required to grant these permission to service accounts
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;watch;list;create
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;watch;list;create;update
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;watch;list;create

// Reconcile compares the scan object against the state of the cluster and updates both if needed
func (r *ScanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("scan", req.NamespacedName)

	// get the scan
	var scan executionv1.Scan
	if err := r.Get(ctx, req.NamespacedName, &scan); err != nil {
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		log.V(7).Info("Unable to fetch Scan")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if scan.Status.State == "" {
		scan.Status.State = executionv1.ScanStateInit
		updateScanStateMetrics(scan)
	}

	log.V(5).Info("Scan Found", "Type", scan.Spec.ScanType, "State", scan.Status.State)

	// Handle Finalizer if the scan is getting deleted
	if !scan.ObjectMeta.DeletionTimestamp.IsZero() {
		// Check if this Scan has not yet been converted to new CRD
		if scan.Status.OrderedHookStatuses == nil && scan.Status.ReadAndWriteHookStatus != nil && scan.Status.State == executionv1.ScanStateDone {
			if err := r.migrateHookStatus(&scan); err != nil {
				return ctrl.Result{}, err
			}
		}
		if err := r.handleFinalizer(&scan); err != nil {
			r.Log.Error(err, "Failed to run Scan Finalizer")
			return ctrl.Result{}, err
		}
	}

	var err error
	switch scan.Status.State {
	case executionv1.ScanStateInit:
		err = r.startScan(&scan)
	case executionv1.ScanStateScanning:
		err = r.checkIfScanIsCompleted(&scan)
	case executionv1.ScanStateScanCompleted:
		err = r.startParser(&scan)
	case executionv1.ScanStateParsing:
		err = r.checkIfParsingIsCompleted(&scan)
	case executionv1.ScanStateParseCompleted:
		err = r.setHookStatus(&scan)
	case executionv1.ScanStateHookProcessing:
		err = r.executeHooks(&scan)
	case executionv1.ScanStateErrored:
		if r.checkIfTTLSecondsAfterFinishedIsCompleted(&scan) {
			err = r.deleteScan(&scan)
		}
	case executionv1.ScanStateDone:
		if r.checkIfTTLSecondsAfterFinishedIsCompleted(&scan) {
			err = r.deleteScan(&scan)
		}
	case executionv1.ScanStateReadAndWriteHookProcessing:
		fallthrough
	case executionv1.ScanStateReadAndWriteHookCompleted:
		fallthrough
	case executionv1.ScanStateReadOnlyHookProcessing:
		err = r.migrateHookStatus(&scan)
	}

	if scan.Spec.TTLSecondsAfterFinished != nil && (scan.Status.State == executionv1.ScanStateDone || scan.Status.State == executionv1.ScanStateErrored) {
		return ctrl.Result{
			Requeue:      true,
			RequeueAfter: time.Duration(*scan.Spec.TTLSecondsAfterFinished) * time.Second,
		}, err
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ScanReconciler) handleFinalizer(scan *executionv1.Scan) error {
	if containsString(scan.ObjectMeta.Finalizers, s3StorageFinalizer) {
		r.Log.V(3).Info("Deleting External Files from FileStorage", "ScanUID", scan.UID)

		err := r.FileStorage.DeleteFile(*scan, scan.Status.RawResultFile)
		if err != nil {
			return err
		}

		err = r.FileStorage.DeleteFile(*scan, "findings.json")
		if err != nil {
			return err
		}

		scan.ObjectMeta.Finalizers = removeString(scan.ObjectMeta.Finalizers, s3StorageFinalizer)
		if err := r.Update(context.Background(), scan); err != nil {
			return err
		}
	}
	return nil
}

func updateScanStateMetrics(scan executionv1.Scan) {
	if scan.Status.State == executionv1.ScanStateInit {
		scansStartedMetric.With(prometheus.Labels{commonMetricLabelScanType: scan.Spec.ScanType}).Inc()
	}
	if scan.Status.State == executionv1.ScanStateErrored {
		scansErroredMetric.With(prometheus.Labels{commonMetricLabelScanType: scan.Spec.ScanType}).Inc()
	}
	if scan.Status.State == executionv1.ScanStateDone {
		scansDoneMetric.With(prometheus.Labels{commonMetricLabelScanType: scan.Spec.ScanType}).Inc()
	}
}

func (r *ScanReconciler) updateScanStatus(ctx context.Context, scan *executionv1.Scan) error {
	updateScanStateMetrics(*scan)
	if scan.Status.State == executionv1.ScanStateDone || scan.Status.State == executionv1.ScanStateErrored {
		if scan.Status.FinishedAt == nil {
			scan.Status.FinishedAt = &metav1.Time{Time: time.Now()}
		}
	}

	if err := r.Status().Update(ctx, scan); err != nil {
		if apierrors.IsConflict(err) {
			r.Log.V(4).Info(
				"Conflict while updating Scan status",
				"scan", scan.Name,
				"namespace", scan.Namespace,
			)
		} else {
			r.Log.Error(err, "unable to update Scan status")
			return err
		}

	}
	return nil
}

// SetupWithManager sets up the controller and initializes every thing it needs
func (r *ScanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctx := context.Background()
	if err := mgr.GetFieldIndexer().IndexField(ctx, &batch.Job{}, ownerKey, func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*batch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// ...make sure it's a CronJob...
		if owner.APIVersion != apiGVStr || owner.Kind != "Scan" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&executionv1.Scan{}).
		Owns(&batch.Job{}).
		Complete(r)
}

func removeString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

// Helper functions to check and remove string from a slice of strings.
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
