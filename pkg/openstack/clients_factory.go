package openstack

import (
	"fmt"
	"os"
	"sync"

	"github.com/gophercloud/gophercloud/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cloud-provider-openstack/pkg/client"
	"k8s.io/klog/v2"
)

const CustomProjectAliasLabel = "shared.salt.x5.ru/project-alias"

const computeClientType = "compute"
const networkClientType = "network"
const loadbalancerClientType = "loadbalancer"
const routesClientType = "routes"
const secretClientType = "secrets"

const configsPath = "/etc/config/"

type clientsFactory struct {
	clientType    string
	defaultClient *gophercloud.ServiceClient
	clients       map[string]*gophercloud.ServiceClient
	m             *sync.Mutex
}

func newClientsFactory(clientType string, defaultClient *gophercloud.ServiceClient) *clientsFactory {
	return &clientsFactory{
		clientType:    clientType,
		defaultClient: defaultClient,
		clients:       make(map[string]*gophercloud.ServiceClient),
		m:             &sync.Mutex{},
	}
}

func (c *clientsFactory) get(meta metav1.ObjectMeta) *gophercloud.ServiceClient {
	if meta.Labels == nil || meta.Labels[CustomProjectAliasLabel] == "" {
		return c.defaultClient
	}
	customProjectAlias := meta.Labels[CustomProjectAliasLabel]
	c.m.Lock()
	defer c.m.Unlock()
	memoryClient, ok := c.clients[c.clientKey(customProjectAlias)]
	if ok {
		return memoryClient
	}
	typedClient, err := c.getProjectTypedClient(customProjectAlias)
	if err != nil {
		klog.Errorf("Failed to get openstack client for project %s: %#v", customProjectAlias, err)
		return c.defaultClient
	}
	c.clients[c.clientKey(customProjectAlias)] = typedClient
	return typedClient
}

func (c *clientsFactory) getProjectTypedClient(projectAlias string) (*gophercloud.ServiceClient, error) {
	cloudConfig, err := c.getProjectConfig(projectAlias)
	if err != nil {
		return nil, fmt.Errorf("failed to read cloud provider configuration %s", err)
	}
	provider, ok, err := c.getProjectProvider(cloudConfig)
	if err != nil {
		klog.Errorf("Couldn't get openstack client for project %s: %#v", projectAlias, err)
		return nil, err
	}
	if !ok {
		klog.Errorf("openstack client not found for project %s: %#v", projectAlias, err)
		return nil, err
	}

	epOpts := &gophercloud.EndpointOpts{
		Region:       cloudConfig.Global.Region,
		Availability: cloudConfig.Global.EndpointType,
	}

	switch c.clientType {
	case computeClientType:
		compute, err := client.NewComputeV2(provider, epOpts)
		if err != nil {
			klog.Errorf("unable to access compute v2 API : %v", err)
			return nil, err
		}
		return compute, nil
	case networkClientType:
		network, err := client.NewNetworkV2(provider, epOpts)
		if err != nil {
			klog.Errorf("Failed to create an OpenStack Network client: %v", err)
			return nil, err
		}
		return network, nil
	case loadbalancerClientType:
		lb, err := client.NewLoadBalancerV2(provider, epOpts)
		if err != nil {
			klog.Errorf("Failed to create an OpenStack LoadBalancer client: %v", err)
			return nil, err
		}
		return lb, nil
	case routesClientType:
		network, err := client.NewNetworkV2(provider, epOpts)
		if err != nil {
			klog.Errorf("Failed to create an OpenStack Network client: %v", err)
			return nil, err
		}
		return network, nil
	case secretClientType:
		secret, err := client.NewKeyManagerV1(provider, epOpts)
		if err != nil {
			klog.Errorf("Failed to create an OpenStack Secret client: %v", err)
			return nil, err
		}
		return secret, nil
	}

	return nil, fmt.Errorf("unknown client type %s", c.clientType)
}

func (c *clientsFactory) getProjectConfig(projectAlias string) (*Config, error) {
	fullConfigPath := c.configPath(projectAlias)
	var config *os.File
	config, err := os.Open(fullConfigPath)
	if err != nil {
		klog.Errorf("Couldn't open cloud provider configuration %s: %#v",
			fullConfigPath, err)
		return nil, fmt.Errorf("failed to open cloud provider configuration %s: %v", fullConfigPath, err)
	}

	defer config.Close()
	cloudConfig, err := ReadConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to read cloud provider configuration %s: %v", fullConfigPath, err)
	}

	return &cloudConfig, nil
}

func (c *clientsFactory) getProjectProvider(cloudConfig *Config) (*gophercloud.ProviderClient, bool, error) {
	provider, err := client.NewOpenStackClient(&cloudConfig.Global, "openstack-cloud-controller-manager", userAgentData...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create openstack client: %v", err)
	}

	return provider, true, nil
}

func (c *clientsFactory) clientKey(projectID string) string {
	return c.clientType + "/" + projectID
}

func (c *clientsFactory) configPath(configName string) string {
	return configsPath + "/" + configName + ".conf"
}
