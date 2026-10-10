# Ray 集群部署

Ray 集群部署步骤已合并到 [项目部署指南](../README.md)，请按统一流程操作。

本目录保留部署配置：

- [raycluster.yaml](./raycluster.yaml)：Ray 集群、default Worker Pool 和控制面只读权限。
- [ray-client-nodeport.yaml](./ray-client-nodeport.yaml)：受控 LAN 的 Ray Client 入口。
