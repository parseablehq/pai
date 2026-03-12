/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package helm

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/repo"
	"k8s.io/client-go/rest"
)

var (
	settings = cli.New()
	mu       sync.Mutex
)

func helmDebugLog(format string, v ...interface{}) {
	// no-op, can be wired to a logger if needed
}

// Client wraps Helm actions
type Client struct {
	actionConfig *action.Configuration
	settings     *cli.EnvSettings
}

// NewClient creates a new Helm client for the given namespace
func NewClient(namespace string, restConfig *rest.Config) (*Client, error) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(RESTClientGetter{
		namespace:  namespace,
		kubeConfig: restConfig,
	}, namespace, os.Getenv("HELM_DRIVER"), helmDebugLog); err != nil {
		return nil, fmt.Errorf("failed to init helm action config: %w", err)
	}

	return &Client{
		actionConfig: actionConfig,
		settings:     settings,
	}, nil
}

// AddRepository adds a Helm chart repository
func (c *Client) AddRepository(name, url string) error {
	mu.Lock()
	defer mu.Unlock()

	repoFile := c.settings.RepositoryConfig

	if err := os.MkdirAll(filepath.Dir(repoFile), 0755); err != nil {
		return fmt.Errorf("failed to create repo config dir: %w", err)
	}

	var file *repo.File
	if _, err := os.Stat(repoFile); err == nil {
		file, err = repo.LoadFile(repoFile)
		if err != nil {
			return fmt.Errorf("failed to load repo file: %w", err)
		}
	} else {
		file = repo.NewFile()
	}

	entry := &repo.Entry{
		Name: name,
		URL:  url,
	}

	r, err := repo.NewChartRepository(entry, getter.All(c.settings))
	if err != nil {
		return fmt.Errorf("failed to create chart repository: %w", err)
	}

	if _, err := r.DownloadIndexFile(); err != nil {
		return fmt.Errorf("failed to download repo index: %w", err)
	}

	file.Update(entry)

	if err := file.WriteFile(repoFile, 0644); err != nil {
		return fmt.Errorf("failed to save repo file: %w", err)
	}

	return nil
}

// InstallChart installs a Helm chart
func (c *Client) InstallChart(ctx context.Context, releaseName, chartRef, namespace string, values map[string]interface{}) error {
	install := action.NewInstall(c.actionConfig)
	install.ReleaseName = releaseName
	install.Namespace = namespace
	install.CreateNamespace = true
	install.Timeout = 10 * time.Minute
	install.Wait = true
	install.WaitForJobs = true

	cp, err := install.ChartPathOptions.LocateChart(chartRef, c.settings)
	if err != nil {
		return fmt.Errorf("failed to locate chart %s: %w", chartRef, err)
	}

	chartRequested, err := loader.Load(cp)
	if err != nil {
		return fmt.Errorf("failed to load chart %s: %w", cp, err)
	}

	if req := chartRequested.Metadata.Dependencies; req != nil {
		if err := action.CheckDependencies(chartRequested, req); err != nil {
			man := &downloader.Manager{
				ChartPath:        cp,
				Keyring:          install.ChartPathOptions.Keyring,
				SkipUpdate:       false,
				Getters:          getter.All(c.settings),
				RepositoryConfig: c.settings.RepositoryConfig,
				RepositoryCache:  c.settings.RepositoryCache,
			}
			if err := man.Update(); err != nil {
				return fmt.Errorf("failed to update chart dependencies: %w", err)
			}
		}
	}

	if _, err := install.RunWithContext(ctx, chartRequested, values); err != nil {
		return fmt.Errorf("failed to install chart %s: %w", releaseName, err)
	}

	return nil
}

// UninstallChart uninstalls a Helm release
func (c *Client) UninstallChart(releaseName string) error {
	uninstall := action.NewUninstall(c.actionConfig)
	uninstall.Wait = true
	uninstall.Timeout = 5 * time.Minute

	if _, err := uninstall.Run(releaseName); err != nil {
		return fmt.Errorf("failed to uninstall release %s: %w", releaseName, err)
	}

	return nil
}

// ReleaseExists checks if a Helm release exists
func (c *Client) ReleaseExists(releaseName string) (bool, error) {
	list := action.NewList(c.actionConfig)
	list.Filter = releaseName
	releases, err := list.Run()
	if err != nil {
		return false, fmt.Errorf("failed to list releases: %w", err)
	}

	for _, r := range releases {
		if r.Name == releaseName {
			return true, nil
		}
	}

	return false, nil
}
