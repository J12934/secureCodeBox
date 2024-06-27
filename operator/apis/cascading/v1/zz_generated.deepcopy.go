//go:build !ignore_autogenerated

// SPDX-FileCopyrightText: the secureCodeBox authors
//
// SPDX-License-Identifier: Apache-2.0

// Code generated by controller-gen. DO NOT EDIT.

package v1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CascadingRule) DeepCopyInto(out *CascadingRule) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CascadingRule.
func (in *CascadingRule) DeepCopy() *CascadingRule {
	if in == nil {
		return nil
	}
	out := new(CascadingRule)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *CascadingRule) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CascadingRuleList) DeepCopyInto(out *CascadingRuleList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]CascadingRule, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CascadingRuleList.
func (in *CascadingRuleList) DeepCopy() *CascadingRuleList {
	if in == nil {
		return nil
	}
	out := new(CascadingRuleList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *CascadingRuleList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CascadingRuleSpec) DeepCopyInto(out *CascadingRuleSpec) {
	*out = *in
	in.Matches.DeepCopyInto(&out.Matches)
	if in.ScanLabels != nil {
		in, out := &in.ScanLabels, &out.ScanLabels
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.ScanAnnotations != nil {
		in, out := &in.ScanAnnotations, &out.ScanAnnotations
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	in.ScanSpec.DeepCopyInto(&out.ScanSpec)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CascadingRuleSpec.
func (in *CascadingRuleSpec) DeepCopy() *CascadingRuleSpec {
	if in == nil {
		return nil
	}
	out := new(CascadingRuleSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *CascadingRuleStatus) DeepCopyInto(out *CascadingRuleStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new CascadingRuleStatus.
func (in *CascadingRuleStatus) DeepCopy() *CascadingRuleStatus {
	if in == nil {
		return nil
	}
	out := new(CascadingRuleStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Matches) DeepCopyInto(out *Matches) {
	*out = *in
	if in.AnyOf != nil {
		in, out := &in.AnyOf, &out.AnyOf
		*out = make([]MatchesRule, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Matches.
func (in *Matches) DeepCopy() *Matches {
	if in == nil {
		return nil
	}
	out := new(Matches)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MatchesRule) DeepCopyInto(out *MatchesRule) {
	*out = *in
	if in.Attributes != nil {
		in, out := &in.Attributes, &out.Attributes
		*out = make(map[string]intstr.IntOrString, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MatchesRule.
func (in *MatchesRule) DeepCopy() *MatchesRule {
	if in == nil {
		return nil
	}
	out := new(MatchesRule)
	in.DeepCopyInto(out)
	return out
}
