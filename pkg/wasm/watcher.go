package wasm

import (
	"os"
	"path/filepath"
	"strings"

	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/wasm"

	"github.com/fsnotify/fsnotify"
)

var (
	watcher     *fsnotify.Watcher
	pluginNames = make(map[string]string)
)

func init() {
	var err error
	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] init fail to create watcher: %v", err)
		return
	}
	go runWatcher()
}

func runWatcher() {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] runWatcher exit")
				return
			}
			log.DefaultLogger.Debugf("[proxywasm] [watcher] runWatcher got event, %s", event)

			if pathIsWasmFile(event.Name) {
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					if fileExist(event.Name) {
						_ = watcher.Add(event.Name)
					} else {
						break
					}
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					if fileExist(event.Name) {
						_ = watcher.Add(event.Name)
					}
				}
				reloadWasm(event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] runWatcher exit")
				return
			}
			log.DefaultLogger.Errorf("[proxywasm] [watcher] runWatcher got errors, err: %v", err)
		}
	}
}

func addWatchFile(cfg *filterConfig, pluginName string) {
	path := cfg.VmConfig.Path
	if err := watcher.Add(path); err != nil {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] addWatchFile fail to watch wasm file, err: %v", err)
		return
	}

	dir := filepath.Dir(path)
	if err := watcher.Add(dir); err != nil {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] addWatchFile fail to watch wasm dir, err: %v", err)
		return
	}

	pluginNames[path] = pluginName
	log.DefaultLogger.Infof("[proxywasm] [watcher] addWatchFile start to watch wasm file and its dir: %s", path)
}

func reloadWasm(fullPath string) {
	found := false

	for path, pluginName := range pluginNames {
		if strings.HasSuffix(fullPath, path) {
			found = true

			err := wasm.GetWasmManager().ReloadWasmByName(pluginName)
			if err != nil {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm fail to add plugin, err: %v", err)
				return
			}

			log.DefaultLogger.Infof("[proxywasm] [watcher] reloadWasm reload wasm success: %s", path)
		}
	}

	if !found {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm WasmPluginConfig not found: %s", fullPath)
	}
}

func fileExist(file string) bool {
	_, err := os.Stat(file)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return true
}

func pathIsWasmFile(fullPath string) bool {
	for path, _ := range pluginNames {
		if strings.HasSuffix(fullPath, path) {
			return true
		}
	}
	return false
}
