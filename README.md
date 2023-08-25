# 项目说明
- 在项目目录使用`mvn clean package`打包
- `bash workpublish/bin/start.sh`启动所有节点
- `bash workpublish/bin/stop.sh`关闭所有节点
- 启动后各节点注册到zookeeper
## metaServer
- 采用jraft实现，至少两节点存活来提供服务
- raft数据、日志及快照存储在metaServer/raft中
- raft快照周期为30s，选举超时为1s
- 共三个节点，HTTP端口为8000/8001/8002，RPC端口为8100/8101/8102
- 实现了fsck+recovery，自动扩展至三副本，恢复周期为20s
## dataServer
- 共四个节点，HTTP端口为9000/9001/9002/9003
- 定时上报周期为30s
## 读写策略
- 读取时先从ds下载文件到本地，然后打开本地文件流读入
- 写入时先通过本地文件流写入到文件，然后上传到ds

# 简单实现一些NAS功能
## 各模块说明
### bin：项目一键启动脚本，用于编译完成后，上传至服务器上，可以将minFS服务整体启动起来
### dataServer:主要提供数据内容存储服务能力，单节点无状态设计，可以横向扩容
### metaServer:主要提供文件系统全局元数据管理，管理dataserver的负载均衡，主备模式运行
### easyClient:一个功能逻辑简单的SDK，用来与metaServer、dataServer通信，完成文件数据操作