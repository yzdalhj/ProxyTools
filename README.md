# VPN 局域网转发

将 HTTP(S) 或 WebSocket（WS/WSS）服务转发到本机局域网地址，供同一网络中的设备访问。

## 使用

1. 启动应用，填入转发网址，例如 `https://httpbin.org` 或 `wss://example.com/socket`。
2. 使用自动获取的局域网 IP，端口建议填 `8080`。
3. 点击“启动转发”。局域网设备访问 `http://局域网IP:端口/路径`。

例如，转发网址为 `https://httpbin.org`、监听地址为 `192.168.1.20:8080` 时，访问：

```text
http://192.168.1.20:8080/get
```

Windows 首次运行时请允许“专用网络”防火墙访问。macOS/Linux 的 `1024` 以下端口需要管理员权限。

## 下载

在 [GitHub Actions](https://github.com/yzdalhj/ProxyTools/actions) 的构件中下载：

- `vpn-assistant-windows-portable`：解压后直接运行 `vpn-lan-proxy.exe`。
- `vpn-assistant-windows`：Windows 安装包。
- `vpn-assistant-macos`：macOS DMG。

## 开发

```bash
npm ci
npm run dev
```
