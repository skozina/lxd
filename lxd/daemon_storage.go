package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/node"
	"github.com/canonical/lxd/lxd/project"
	"github.com/canonical/lxd/lxd/rsync"
	"github.com/canonical/lxd/lxd/state"
	storagePools "github.com/canonical/lxd/lxd/storage"
	storageDrivers "github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
)

func daemonStorageVolumesUnmount(s *state.State, ctx context.Context) error {
	var storageBackups string
	var storageImages string

	err := s.DB.Node.Transaction(ctx, func(ctx context.Context, tx *db.NodeTx) error {
		nodeConfig, err := node.ConfigLoad(ctx, tx)
		if err != nil {
			return err
		}

		storageBackups = nodeConfig.StorageBackupsVolume()
		storageImages = nodeConfig.StorageImagesVolume()

		return nil
	})
	if err != nil {
		return err
	}

	unmount := func(source string) error {
		// Parse the source.
		poolName, volumeName, err := daemonStorageSplitVolume(source)
		if err != nil {
			return err
		}

		pool, err := storagePools.LoadByName(s, poolName)
		if err != nil {
			return err
		}

		// Unmount volume.
		_, err = pool.UnmountCustomVolume(api.ProjectDefaultName, volumeName, nil)
		if err != nil && !errors.Is(err, storageDrivers.ErrInUse) {
			return fmt.Errorf("Failed to unmount storage volume %q: %w", source, err)
		}

		return nil
	}

	select {
	case <-ctx.Done():
		return errors.New("Timed out waiting for image and backup volume")
	default:
		if storageBackups != "" {
			err := unmount(storageBackups)
			if err != nil {
				return fmt.Errorf("Failed to unmount backups storage: %w", err)
			}
		}

		if storageImages != "" {
			err := unmount(storageImages)
			if err != nil {
				return fmt.Errorf("Failed to unmount images storage: %w", err)
			}
		}
	}

	return nil
}

func daemonStorageMount(s *state.State) error {
	var storageBackups string
	var storageImages string
	err := s.DB.Node.Transaction(context.TODO(), func(ctx context.Context, tx *db.NodeTx) error {
		nodeConfig, err := node.ConfigLoad(ctx, tx)
		if err != nil {
			return err
		}

		storageBackups = nodeConfig.StorageBackupsVolume()
		storageImages = nodeConfig.StorageImagesVolume()

		return nil
	})
	if err != nil {
		return err
	}

	mount := func(source string) error {
		// Parse the source.
		poolName, volumeName, err := daemonStorageSplitVolume(source)
		if err != nil {
			return err
		}

		pool, err := storagePools.LoadByName(s, poolName)
		if err != nil {
			return err
		}

		// Mount volume.
		_, err = pool.MountCustomVolume(api.ProjectDefaultName, volumeName, nil)
		if err != nil {
			return fmt.Errorf("Failed to mount storage volume %q: %w", source, err)
		}

		return nil
	}

	if storageBackups != "" {
		err := mount(storageBackups)
		if err != nil {
			return fmt.Errorf("Failed to mount backups storage: %w", err)
		}
	}

	if storageImages != "" {
		err := mount(storageImages)
		if err != nil {
			return fmt.Errorf("Failed to mount images storage: %w", err)
		}
	}

	return nil
}

func daemonStorageSplitVolume(volume string) (poolName string, volumeName string, err error) {
	if strings.Count(volume, "/") != 1 {
		return "", "", errors.New("Invalid syntax for volume, must be <pool>/<volume>")
	}

	poolName, volumeName, _ = strings.Cut(volume, "/")

	// Validate pool name.
	if strings.Contains(poolName, "\\") || strings.Contains(poolName, "..") {
		return "", "", fmt.Errorf("Invalid pool name: %q", poolName)
	}

	// Validate volume name.
	if strings.Contains(volumeName, "\\") || strings.Contains(volumeName, "..") {
		return "", "", fmt.Errorf("Invalid volume name: %q", volumeName)
	}

	return poolName, volumeName, nil
}

// daemonStorageValidate checks target "<poolName>/<volumeName>" value and returns the validated target from DB.
func daemonStorageValidate(s *state.State, target string) (validatedTarget string, err error) {
	// Check syntax.
	if target == "" {
		return "", nil
	}

	poolName, volumeName, err := daemonStorageSplitVolume(target)
	if err != nil {
		return "", err
	}

	// Validate pool exists.
	pool, err := storagePools.LoadByName(s, poolName)
	if err != nil {
		return "", fmt.Errorf("Failed loading storage pool %q: %w", poolName, err)
	}

	poolState := pool.Status()
	if poolState != api.StoragePoolStatusCreated {
		return "", fmt.Errorf("Storage pool %q cannot be used when in %q status", poolName, poolState)
	}

	// Checking only for remote storage drivers isn't sufficient as drivers
	// like CephFS can be safely used as the volume can be used on multiple nodes concurrently.
	if pool.Driver().Info().Remote && !pool.Driver().Info().VolumeMultiNode {
		return "", fmt.Errorf("Remote storage pool %q cannot be used", poolName)
	}

	var snapshots []db.StorageVolumeArgs
	var dbVol *db.StorageVolume

	err = s.DB.Cluster.Transaction(s.ShutdownCtx, func(ctx context.Context, tx *db.ClusterTx) error {
		// Confirm volume exists.
		dbVol, err = tx.GetStoragePoolVolume(ctx, pool.ID(), api.ProjectDefaultName, cluster.StoragePoolVolumeTypeCustom, volumeName, true)
		if err != nil {
			return fmt.Errorf("Failed loading storage volume %q in %q project: %w", target, api.ProjectDefaultName, err)
		}

		if dbVol.ContentType != cluster.StoragePoolVolumeContentTypeNameFS {
			return fmt.Errorf("Storage volume %q in %q project is not filesystem content type", target, api.ProjectDefaultName)
		}

		snapshots, err = tx.GetLocalStoragePoolVolumeSnapshotsWithType(ctx, api.ProjectDefaultName, volumeName, cluster.StoragePoolVolumeTypeCustom, pool.ID())
		if err != nil {
			return fmt.Errorf("Unable to load storage volume snapshots %q in %q project: %w", target, api.ProjectDefaultName, err)
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	if len(snapshots) != 0 {
		return "", errors.New("Storage volumes for use by LXD itself cannot have snapshots")
	}

	// Mount volume.
	_, err = pool.MountCustomVolume(api.ProjectDefaultName, volumeName, nil)
	if err != nil {
		return "", fmt.Errorf("Failed to mount storage volume %q: %w", target, err)
	}

	defer func() { _, _ = pool.UnmountCustomVolume(api.ProjectDefaultName, volumeName, nil) }()

	// Validate volume is empty (ignore lost+found).
	volStorageName := project.StorageVolume(api.ProjectDefaultName, volumeName)
	mountpoint := storageDrivers.GetVolumeMountPath(poolName, storageDrivers.VolumeTypeCustom, volStorageName)

	entries, err := os.ReadDir(mountpoint)
	if err != nil {
		return "", fmt.Errorf("Failed to list %q: %w", mountpoint, err)
	}

	allowedEntries := []string{
		"lost+found", // Clean ext4 volumes.
		".zfs",       // Systems with snapdir=visible
		"images",
		"backups", // Allow re-use of volume for multiple images and backups stores.
	}

	for _, entry := range entries {
		entryName := entry.Name()

		// Don't fail on entries known to be possibly present.
		if slices.Contains(allowedEntries, entryName) {
			continue
		}

		return "", fmt.Errorf("Storage volume %q isn't empty", target)
	}

	return pool.Name() + "/" + dbVol.Name, nil
}

func daemonStoragePath(config string, target string) string {
	if config == "" {
		return shared.VarPath(target)
	}

	poolName, volumeName, _ := daemonStorageSplitVolume(config)
	volStorageName := project.StorageVolume(api.ProjectDefaultName, volumeName)
	volMountPath := storageDrivers.GetVolumeMountPath(poolName, storageDrivers.VolumeTypeCustom, volStorageName)
	return filepath.Join(volMountPath, target)
}

// Return storage configured for the project, or the storage configured for the daemon if project doesn't have any.
func projectStorageVolume(s *state.State, p string, projectConfigs map[string]string) string {
	config, ok := projectConfigs[p]
	if ok {
		return config
	}

	return s.LocalConfig.StorageImagesVolume()
}

func imageFilesInDaemonStorage(s *state.State, oldConfig string) (error, []string, []string) {
	toCopy := make(map[string]bool)
	toKeep := make(map[string]bool)
	err := s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		images, err := cluster.GetImages(ctx, tx.Tx())
		if err != nil {
			return fmt.Errorf("failed to fetch images: %w", err)
		}

		configs, err := cluster.ProjectConfigValues(ctx, tx.Tx(), "storage.images_volume")
		if err != nil {
			return fmt.Errorf("failed to fetch project configs: %w", err)
		}

		for _, image := range images {
			projectConfig, ok := configs[image.Project]
			if ok {
				// If there's a project which needs an image in its current location, we can't delete it.
				if projectConfig == oldConfig {
					toKeep[image.Fingerprint] = true
				}

				// If the image project has its own storage, there's no need to move it.
				continue
			}

			toCopy[image.Fingerprint] = true
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to fetch project files: %w", err), nil, nil
	}

	// Convert the sets to output lists.
	resultCopyList := make([]string, 0, len(toCopy))
	resultDeleteList := make([]string, 0, len(toCopy))
	for key := range toCopy {
		resultCopyList = append(resultCopyList, key)

		if _, ok := toKeep[key]; !ok {
			resultDeleteList = append(resultDeleteList, key)
		}
	}

	return nil, resultCopyList, resultDeleteList
}

func projectImageFiles(s *state.State, p string, oldConfig string) (error, []string, []string) {
	var toCopy, toDelete []string

	// If our project didn't had the config defined, but the daemon had, lets pretend that's our prevous configured storage.
	if oldConfig == "" && s.LocalConfig.StorageImagesVolume() != "" {
		oldConfig = s.LocalConfig.StorageImagesVolume()
	}

	err := s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		images, err := cluster.GetImages(ctx, tx.Tx())
		if err != nil {
			return fmt.Errorf("failed to fetch images: %w", err)
		}

		configs, err := cluster.ProjectConfigValues(ctx, tx.Tx(), "storage.images_volume")
		if err != nil {
			return fmt.Errorf("failed to fetch project configs: %w", err)
		}

		// Build a list of our project images as well as a set of other projects using each image.
		imagesInProjects := make(map[string]map[string]bool)
		toCopy = make([]string, 0, len(images))
		for _, image := range images {
			if image.Project == p {
				toCopy = append(toCopy, image.Fingerprint)
				continue
			}

			if imagesInProjects[image.Fingerprint] == nil {
				imagesInProjects[image.Fingerprint] = make(map[string]bool, 0)
			}

			imagesInProjects[image.Fingerprint][image.Project] = true
		}

		// Find which of the images we can safely delete.
		toDelete = make([]string, 0, len(images))
	OUTER:
		for _, image := range toCopy {
			otherProjects, ok := imagesInProjects[image]

			// If we're the sole user of the image, we can delete it.
			if !ok {
				toDelete = append(toDelete, image)
				continue
			}

			for otherProject := range otherProjects {
				// If other project using this image use the same storage, we can't delete.
				if projectStorageVolume(s, otherProject, configs) == oldConfig {
					continue OUTER
				}
			}

			toDelete = append(toDelete, image)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to fetch project files: %w", err), nil, nil
	}

	return nil, toCopy, toDelete
}

func daemonStorageConfig(s *state.State, storageType string) string {
	result := ""
	switch storageType {
	case "backups":
		result = s.LocalConfig.StorageBackupsVolume()
	case "images":
		result = s.LocalConfig.StorageImagesVolume()
	}

	return result
}

func daemonStorageMove(s *state.State, storageType string, oldConfig string, newconfig string, p string) error {
	var destPath string
	var sourcePath string
	var sourcePool string
	var sourceVolume string
	var err error

	moveContent := func(source string, target string, filesToCopy []string, filesToDelete []string) (err error) {
		if filesToCopy != nil {
			// Copy only selected files.
			for _, file := range filesToCopy {
				fmt.Println("ERSIN do copy image ", file)
				_, err = rsync.CopyFile(filepath.Join(source, file), target, "", false)
				if err != nil {
					break
				}

				_, err = rsync.CopyFile(filepath.Join(source, file+".rootfs"), target, "", false)
				if err != nil {
					break
				}
			}
		} else {
			// Copy everything.
			_, err = rsync.LocalCopy(source, target, "", false)
		}

		if err != nil {
			return err
		}

		if filesToDelete != nil {
			for _, file := range filesToDelete {
				fmt.Println("ERSIN do delete image ", file)

				err = os.Remove(filepath.Join(source, file))
				if err != nil {
					break
				}

				err = os.Remove(filepath.Join(source, file+".rootfs"))
				if err != nil {
					break
				}
			}
		} else {
			err = os.RemoveAll(source)
		}

		return err
	}

	// Track down the current storage.
	if newconfig == "" {
		if p == "" {
			destPath = shared.VarPath(storageType)
		} else {
			destPath = daemonStoragePath(daemonStorageConfig(s, storageType), storageType)
		}
		fmt.Println("ERSIN newConfig empty, destPath: ", destPath)
	}

	if oldConfig == "" {
		if p == "" {
			sourcePath = shared.VarPath(storageType)
		} else {
			sourcePath = daemonStoragePath(daemonStorageConfig(s, storageType), storageType)
		}
		fmt.Println("ERSIN oldConfig empty, sourcePath: ", sourcePath)
	} else {
		sourcePool, sourceVolume, err = daemonStorageSplitVolume(oldConfig)

		if err != nil {
			return err
		}

		sourceVolume = project.StorageVolume(api.ProjectDefaultName, sourceVolume)
		sourcePath = storageDrivers.GetVolumeMountPath(sourcePool, storageDrivers.VolumeTypeCustom, sourceVolume)
		sourcePath = filepath.Join(sourcePath, storageType)
	}

	// Things already look correct.
	if sourcePath == destPath {
		return nil
	}

	// Deal with unsetting.
	if newconfig == "" {
		// Re-create the directory.
		if _, err = os.Stat(destPath); errors.Is(err, os.ErrNotExist) {
			fmt.Println("ERSIN destPath doesn't exist, create: ", destPath)
			err := os.MkdirAll(destPath, 0700)
			if err != nil {
				return fmt.Errorf("Failed to create directory %q: %w", destPath, err)
			}
		}

		// Move the data across.
		var toCopy, toDelete []string
		if p != "" {
			err, toCopy, toDelete = projectImageFiles(s, p, oldConfig)
		} else {
			err, toCopy, toDelete = imageFilesInDaemonStorage(s, oldConfig)
		}

		if err != nil {
			return err
		}

		err = moveContent(sourcePath, destPath, toCopy, toDelete)
		if err != nil {
			return fmt.Errorf("Failed to move data over to directory %q: %w", destPath, err)
		}

		pool, err := storagePools.LoadByName(s, sourcePool)
		if err != nil {
			return err
		}

		// Unmount old volume if noone else is using it.
		projectName, sourceVolumeName := project.StorageVolumeParts(sourceVolume)
		_, err = pool.UnmountCustomVolume(projectName, sourceVolumeName, nil)
		if err != nil && !errors.Is(err, storageDrivers.ErrInUse) {
			return fmt.Errorf(`Failed to umount storage volume "%s/%s": %w`, sourcePool, sourceVolumeName, err)
		}

		return nil
	}

	// Parse the target.
	poolName, volumeName, err := daemonStorageSplitVolume(newconfig)
	if err != nil {
		return err
	}

	fmt.Println("ERSIN poolname, volumename: ", poolName, volumeName)
	pool, err := storagePools.LoadByName(s, poolName)
	if err != nil {
		return err
	}

	// Things already look correct.
	destPath = daemonStoragePath(newconfig, storageType)
	if sourcePath == destPath {
		return nil
	}

	// Mount volume.
	_, err = pool.MountCustomVolume(api.ProjectDefaultName, volumeName, nil)
	if err != nil {
		return fmt.Errorf("Failed to mount storage volume %q: %w", newconfig, err)
	}

	// Ensure the destination directory structure exists within the target volume.
	err = os.MkdirAll(destPath, 0700)
	if err != nil {
		return fmt.Errorf("Failed to create directory %q: %w", destPath, err)
	}

	// Set ownership & mode.
	err = os.Chmod(destPath, 0700)
	if err != nil {
		return fmt.Errorf("Failed to set permissions on %q: %w", destPath, err)
	}

	err = os.Chown(destPath, 0, 0)
	if err != nil {
		return fmt.Errorf("Failed to set ownership on %q: %w", destPath, err)
	}

	// Handle changes.
	if oldConfig != "" {
		// Move the data across.
		var toCopy, toDelete []string
		if p != "" {
			err, toCopy, toDelete = projectImageFiles(s, p, oldConfig)
		} else {
			err, toCopy, toDelete = imageFilesInDaemonStorage(s, oldConfig)
		}

		if err != nil {
			return err
		}

		err = moveContent(sourcePath, destPath, toCopy, toDelete)
		if err != nil {
			return fmt.Errorf("Failed to move data over to directory %q: %w", destPath, err)
		}

		fmt.Println("ERSIN unmount pool: ", sourcePool)
		pool, err := storagePools.LoadByName(s, sourcePool)
		if err != nil {
			return err
		}

		// Unmount old volume.
		projectName, sourceVolumeName := project.StorageVolumeParts(sourceVolume)
		_, err = pool.UnmountCustomVolume(projectName, sourceVolumeName, nil)
		if err != nil && !errors.Is(err, storageDrivers.ErrInUse) {
			return fmt.Errorf(`Failed to umount storage volume "%s/%s": %w`, sourcePool, sourceVolumeName, err)
		}

		return nil
	}

	// Move the data across.
	// For the daemon images we need to move everything, so there's no need to bother with making the lists.
	var toCopy, toDelete []string
	toDelete = make([]string, 0)
	if p != "" {
		err, toCopy, toDelete = projectImageFiles(s, p, oldConfig)
		if err != nil {
			return err
		}
	}

	err = moveContent(sourcePath, destPath, toCopy, toDelete)
	if err != nil {
		return fmt.Errorf("Failed to move data over to directory %q: %w", destPath, err)
	}

	return nil
}
