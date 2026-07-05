const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("gateway", {
  getBackendInfo: () => ipcRenderer.invoke("gateway:getBackendInfo"),
  openExternal: (url) => ipcRenderer.invoke("gateway:openExternal", url),
  quit: () => ipcRenderer.invoke("gateway:quit")
});
