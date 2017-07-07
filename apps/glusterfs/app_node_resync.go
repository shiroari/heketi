//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package glusterfs

import (
	"errors"
	"net/http"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
)

type deviceRev struct {
	id           string
	name         string
	nodeId       string
	manageHost   string
	totalSize    uint64
	newTotalSize uint64
}

func (a *App) NodeResync(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	nodeId := vars["id"]

	revMap := make(map[string]*deviceRev)

	// Get devices on node
	err := a.db.View(func(tx *bolt.Tx) error {
		node, err := NewNodeEntryFromId(tx, nodeId)
		if err != nil {
			return err
		}
		for _, deviceId := range node.Devices {
			device, err := NewDeviceEntryFromId(tx, deviceId)
			if err != nil {
				if err == ErrNotFound {
					continue
				}
				return err
			}
			revMap[device.Id()] = &deviceRev{
				id:           device.Info.Id,
				name:         device.Info.Name,
				nodeId:       device.NodeId,
				manageHost:   node.ManageHostName(),
				totalSize:    device.Info.Storage.Total,
				newTotalSize: 0,
			}
		}
		return nil
	})
	if err == ErrNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		logger.Err(err)
		return
	}

	if len(revMap) == 0 {
		logger.Info("No device found on node on node %v", nodeId)
		return
	}

	logger.Info("Found %v device(s) on node %v. Checking for changes", len(revMap), nodeId)

	// Resync in background
	a.asyncManager.AsyncHttpRedirectFunc(w, r, func() (seeOtherUrl string, e error) {
		err := a.execResync(revMap)
		if err != nil {
			logger.Err(err)
		}
		return "", err
	})
}

func (a *App) execResync(revMap map[string]*deviceRev) error {

	for _, rev := range revMap {
		info, err := a.executor.GetDeviceInfo(rev.manageHost, rev.name, rev.id)
		if err != nil {
			return err
		}
		if rev.totalSize != info.Size {
			logger.Debug("Device '%v' (%v) has changed %v -> %v", rev.name, rev.id,
				rev.totalSize, info.Size)
			rev.newTotalSize = info.Size
		} else {
			delete(revMap, rev.id)
		}
	}

	if len(revMap) == 0 {
		logger.Info("All devices are up to date")
		return nil
	}

	err := a.db.Update(func(tx *bolt.Tx) error {

		for _, rev := range revMap {

			device, err := NewDeviceEntryFromId(tx, rev.id)
			if err != nil {
				if err == ErrNotFound {
					delete(revMap, rev.id)
					continue
				}
				return err
			}

			newFreeSize := rev.newTotalSize - device.Info.Storage.Used

			if newFreeSize < 0 {
				return errors.New("negative free space on device")
			}

			logger.Info("Updating device %v. Total: %v -> %v. Free: %v -> %v", device.Info.Name,
				device.Info.Storage.Total, rev.newTotalSize,
				device.Info.Storage.Free, newFreeSize)

			device.Info.Storage.Total = rev.newTotalSize
			device.Info.Storage.Free = newFreeSize

			err = device.Save(tx)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("Updated %v device(s)", len(revMap))

	return nil
}
