package wasm

import (
	"strings"

	v2 "mosn.io/mosn/pkg/config/v2"
	"mosn.io/mosn/pkg/log"
	"mosn.io/mosn/pkg/wasm"

	"github.com/rjeczalik/notify"
)

var (
	c           = make(chan notify.EventInfo, 8)
	vmConfigs   = make(map[string]*filterConfig)
	pluginNames = make(map[string]string)
)

func init() {
	go runWatcher()
}

func runWatcher() {
	for ei := range c {
		log.DefaultLogger.Infof("[proxywasm] [watcher] runWatcher got events: %s %s", ei.Event(), ei.Path())
		reloadWasm(ei.Path())
	}
}

func addWatchFile(cfg *filterConfig, pluginName string) {
	path := cfg.VmConfig.Path
	if err := notify.Watch(path, c, notify.All, notify.InMovedTo, notify.InCreate, notify.InModify); err != nil {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] addWatchFile fail to watch wasm file, err: %v", err)
		return
	}

	vmConfigs[path] = cfg
	pluginNames[path] = pluginName
	log.DefaultLogger.Infof("[proxywasm] [watcher] addWatchFile start to watch wasm file: %s", path)
}

func reloadWasm(fullPath string) {
	found := false

	for path, config := range vmConfigs {
		if strings.HasSuffix(fullPath, path) {
			found = true

			pluginName := pluginNames[path]

			err := wasm.GetWasmManager().UninstallWasmPluginByName(pluginName)
			if err != nil {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm fail to uninstall plugin, err: %v", err)
			}

			v2Config := v2.WasmPluginConfig{
				PluginName:  pluginName,
				VmConfig:    config.VmConfig,
				InstanceNum: config.InstanceNum,
			}
			err = wasm.GetWasmManager().AddOrUpdateWasm(v2Config)
			if err != nil {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm fail to add plugin, err: %v", err)
				return
			}

			pw := wasm.GetWasmManager().GetWasmPluginWrapperByName(pluginName)
			if pw == nil {
				log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm plugin not found")
				return
			}

			factory := &FilterConfigFactory{
				pluginName: pluginName,
				config:     config,
			}

			pw.RegisterPluginHandler(factory)
			log.DefaultLogger.Infof("[proxywasm] [watcher] reloadWasm reload wasm success: %s", path)
		}
	}

	if !found {
		log.DefaultLogger.Errorf("[proxywasm] [watcher] reloadWasm WasmPluginConfig not found: %s", fullPath)
	}
}
