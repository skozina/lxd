package workflows

import (
	"context"
	"fmt"
	"time"

	"github.com/canonical/lxd/lxd/cluster"
	"github.com/canonical/lxd/lxd/config"
	"github.com/canonical/lxd/lxd/db"
	dbCluster "github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/node"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"

	workflowClient "github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/workflow"
)

const (
	ExtendProjectStorageSchemaWorkflowID = "create-project"
)

var localClusterAddress string

func GetClusterNodesActivity(ctx context.Context) ([]db.NodeInfo, error) {
	fmt.Println("Getting cluster nodes")
	s := StateFunc()
	networkCert := s.Endpoints.NetworkCert()
	serverCert := s.ServerCert()

	offlineThreshold := s.GlobalConfig.OfflineThreshold()

	var members []db.NodeInfo
	err := s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		var err error
		members, err = tx.GetNodes(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}

	// Filter out ourselves and nodes that are offline.
	peers := make([]db.NodeInfo, 0, len(members)-1)
	for _, member := range members {
		if member.IsOffline(offlineThreshold) {
			// Even if the heartbeat timestamp is not recent
			// enough, let's try to connect to the node, just in
			// case the heartbeat is lagging behind for some reason
			// and the node is actually up.
			if !cluster.HasConnectivity(networkCert, serverCert, member.Address) {
				continue // Just skip this node
			}
		}

		peers = append(peers, member)
	}

	return peers, nil
}

func ExtendLocalConfigSchemaForProject(projectName string) {
	// Extend the node config schema with the project-specific config keys.
	// Otherwise the node config schema validation will not allow setting of these keys.
	node.ConfigSchema.Lock()
	node.ConfigSchema.Types["storage.project."+projectName+".images_volume"] = config.Key{}
	node.ConfigSchema.Types["storage.project."+projectName+".backups_volume"] = config.Key{}
	node.ConfigSchema.Unlock()
}

func ExtendProjectStorageSchemaActivity(ctx context.Context, peer db.NodeInfo, project api.ProjectsPost) error {
	fmt.Println("Extending project storage schema on peer", peer.Name, "for project", project.Name)
	s := StateFunc()

	// Don't bother connecting to ourself, just handle the things locally.
	if peer.Address == localClusterAddress || peer.Address == "0.0.0.0" {
		ExtendLocalConfigSchemaForProject(project.Name)
		return nil
	}

	networkCert := s.Endpoints.NetworkCert()
	serverCert := s.ServerCert()
	client, err := cluster.Connect(ctx, peer.Address, networkCert, serverCert, true)
	if err != nil {
		return fmt.Errorf("Failed to connect to peer %s at %s: %w", peer.Name, peer.Address, err)
	}

	err = client.CreateProject(project)
	if err != nil {
		return fmt.Errorf("Failed to notify peer %s at %s: %w", peer.Name, peer.Address, err)
	}

	return nil
}

// Create the default profile of a project.
func ProjectCreateDefaultProfile(ctx context.Context, tx *db.ClusterTx, project string, storagePool string, network string) error {
	// Create a default profile
	profile := dbCluster.Profile{}
	profile.Project = project
	profile.Name = api.ProjectDefaultName
	profile.Description = "Default LXD profile for project " + project

	profileID, err := dbCluster.CreateProfile(ctx, tx.Tx(), profile)
	if err != nil {
		return fmt.Errorf("Add default profile to database: %w", err)
	}

	devices := map[string]dbCluster.Device{}
	if storagePool != "" {
		rootDev := map[string]string{}
		rootDev["path"] = "/"
		rootDev["pool"] = storagePool
		device := dbCluster.Device{
			Name:   "root",
			Type:   dbCluster.TypeDisk,
			Config: rootDev,
		}

		devices["root"] = device
	}

	if network != "" {
		networkDev := map[string]string{}
		networkDev["network"] = network
		device := dbCluster.Device{
			Name:   "eth0",
			Type:   dbCluster.TypeNIC,
			Config: networkDev,
		}

		devices["eth0"] = device
	}

	if len(devices) > 0 {
		err = dbCluster.CreateProfileDevices(ctx, tx.Tx(), profileID, devices)
		if err != nil {
			return fmt.Errorf("Add root device to default profile of new project: %w", err)
		}
	}

	return nil
}

func CreateProjectInDBActivity(ctx context.Context, project api.ProjectsPost) error {
	fmt.Println("Creating project in database:", project.Name)
	s := StateFunc()

	err := s.DB.Cluster.Transaction(ctx, func(ctx context.Context, tx *db.ClusterTx) error {
		id, err := dbCluster.CreateProject(ctx, tx.Tx(), dbCluster.Project{Description: project.Description, Name: project.Name})
		if err != nil {
			return fmt.Errorf("Failed adding database record: %w", err)
		}

		err = dbCluster.CreateProjectConfig(ctx, tx.Tx(), id, project.Config)
		if err != nil {
			return fmt.Errorf("Unable to create project config for project %q: %w", project.Name, err)
		}

		if shared.IsTrue(project.Config["features.profiles"]) {
			err = ProjectCreateDefaultProfile(ctx, tx, project.Name, project.StoragePool, project.Network)
			if err != nil {
				return err
			}

			if project.Config["features.images"] == "false" {
				err = dbCluster.InitProjectWithoutImages(ctx, tx.Tx(), project.Name)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func ExtendProjectStorageSchemaWorkflow(ctx workflow.Context, project api.ProjectsPost) (err error) {
	fmt.Println("Extending project storage schema for project", project.Name)
	s := StateFunc()
	localClusterAddress = s.LocalConfig.ClusterAddress()

	// Get list of peers, no need to compensate if this fails.
	peers, err := workflow.ExecuteActivity[[]db.NodeInfo](ctx, workflow.DefaultActivityOptions, GetClusterNodesActivity).Get(ctx)
	if err != nil {
		return err
	}

	// Notify all peers in parallel.
	localSchemaExtensionFailed := false
	activities := make([]workflow.Future[any], 0, len(peers))
	for _, peer := range peers {
		activities = append(activities, workflow.ExecuteActivity[any](ctx, workflow.DefaultActivityOptions, ExtendProjectStorageSchemaActivity, peer, project))
	}

	for i, activity := range activities {
		workflow.Await(activity, func(ctx workflow.Context, f workflow.Future[any]) {
			_, err := f.Get(ctx)
			if err != nil {
				localSchemaExtensionFailed = true
			}

			fmt.Println("Extended project schema on cluster member", peers[i].Name)
		})
	}

	if localSchemaExtensionFailed {
		return fmt.Errorf("Failed to extend project schema on some cluster members")
	}

	_, err = workflow.ExecuteActivity[interface{}](ctx, workflow.DefaultActivityOptions, CreateProjectInDBActivity, project).Get(ctx)
	return err
}

func CreateProjectWithWorkflow(ctx context.Context, c *workflowClient.Client, project api.ProjectsPost) error {
	wf, err := c.CreateWorkflowInstance(ctx, workflowClient.WorkflowInstanceOptions{
		InstanceID: ExtendProjectStorageSchemaWorkflowID + project.Name,
	}, ExtendProjectStorageSchemaWorkflow, project)
	if err != nil {
		return fmt.Errorf("Workflow failed to start: %w", err)
	}

	_, err = workflowClient.GetWorkflowResult[string](ctx, c, wf, time.Second*10)
	if err != nil {
		return fmt.Errorf("Failed to get workflow result: %w", err)
	}

	return nil
}
