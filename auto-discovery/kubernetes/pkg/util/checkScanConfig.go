// SPDX-FileCopyrightText: the secureCodeBox authors
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"

	config "github.com/secureCodeBox/secureCodeBox/auto-discovery/kubernetes/pkg/config"
	"k8s.io/utils/strings/slices"
)

func CheckUniquenessOfScanNames(scanConfigs []config.ScanConfig) error {
	var namesSeen []string
	for _, config := range scanConfigs {
		if slices.Contains(namesSeen, config.Name) {
			return fmt.Errorf("scan names %s are not unique", config.Name)
		}
		namesSeen = append(namesSeen, config.Name)
	}
	return nil
}
