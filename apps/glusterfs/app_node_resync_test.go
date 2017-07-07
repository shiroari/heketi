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
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/heketi/tests"
	"github.com/heketi/utils"
)

func TestNodeSync(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	var total, used, newTotal uint64
	total = 200 * 1024 * 1024
	used = 1024
	newTotal = 500 * 1024 * 1024 // see mockexec

	nodeId := utils.GenUUID()
	deviceId := utils.GenUUID()

	// Init test database
	err := app.db.Update(func(tx *bolt.Tx) error {
		cluster := NewClusterEntry()
		cluster.Info.Id = utils.GenUUID()
		if err := cluster.Save(tx); err != nil {
			return err
		}

		device := NewDeviceEntry()
		device.Info.Id = deviceId
		device.Info.Name = "/dev/a"
		device.NodeId = nodeId
		device.StorageSet(total)
		device.StorageAllocate(used)

		if err := device.Save(tx); err != nil {
			return err
		}

		node := NewNodeEntry()
		node.Info.Id = nodeId
		node.Info.ClusterId = cluster.Info.Id
		node.Info.Hostnames.Manage = sort.StringSlice{"manage.system"}
		node.Info.Hostnames.Storage = sort.StringSlice{"storage.system"}
		node.Info.Zone = 10

		node.DeviceAdd(device.Info.Id)

		if err := node.Save(tx); err != nil {
			return err
		}

		return nil
	})
	tests.Assert(t, err == nil)

	r, err := http.Get(ts.URL + "/nodes/" + nodeId + "/resync")
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusAccepted)

	location, err := r.Location()
	tests.Assert(t, err == nil)

	for {
		r, err := http.Get(location.String())
		tests.Assert(t, err == nil)
		if r.Header.Get("X-Pending") == "true" {
			tests.Assert(t, r.StatusCode == http.StatusOK)
			time.Sleep(time.Millisecond * 10)
			continue
		} else {
			tests.Assert(t, r.StatusCode == http.StatusNoContent)
			break
		}
	}

	err = app.db.View(func(tx *bolt.Tx) error {
		device, err := NewDeviceEntryFromId(tx, deviceId)
		tests.Assert(t, err == nil)
		tests.Assert(t, device.Info.Storage.Total == newTotal)
		tests.Assert(t, device.Info.Storage.Free == newTotal-used)
		tests.Assert(t, device.Info.Storage.Used == used)
		return nil
	})
	tests.Assert(t, err == nil)
}

func TestNodeSyncIdNotFound(t *testing.T) {
	tmpfile := tests.Tempfile()
	defer os.Remove(tmpfile)

	// Create the app
	app := NewTestApp(tmpfile)
	defer app.Close()
	router := mux.NewRouter()
	app.SetRoutes(router)

	// Setup the server
	ts := httptest.NewServer(router)
	defer ts.Close()

	nodeId := utils.GenUUID()

	// Get unknown node id
	r, err := http.Get(ts.URL + "/nodes/" + nodeId + "/resync")
	tests.Assert(t, err == nil)
	tests.Assert(t, r.StatusCode == http.StatusNotFound)
}
