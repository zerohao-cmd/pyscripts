该项目是一个基于ray为分布式调度底座实现的无状态python脚本任务托管平台

# 项目架构
## 用户界面
用户界面是一个前后端项目, 用户可以在这里注册, 启用, 停用服务, 并且查看服务的运行状态和运行日志

## manager
manager是一个常驻服务,  
其职能
1. 负责定时自动获取最新的脚本数据   
2. 把"脚本解析"为ray的task或者actor服务   
3. 把接口改动传递给api server

manager是项目的核心组件, 其需要对接脚本层, 服务层以及ray调度层  
需要把从git仓库拉取到的脚本, 解析为ray可以接受的任务, 以及restful api或者 grpc api.

## api服务
api server从manager那里获取到最新的接口数据, 并且启动接口.  接口调用会直接被转到ray执行层, 不经过manager.
同时api server需要记录接口调用的记录以及接口状态. 

## ray执行层
k8s提供的ray执行集群

## 数据库
使用postgresql作为后端数据库, 储存api调用记录, 以及部分持久化信息.

## script_hook库
这个python库用于方便用户在代码里面使用装饰器hook函数作为api, 类似:
```python
from script_hook import hook_api, call
import asyncio
import time

# 显式设置服务id, id不能和已有的id重复
@hook_api(api_type='grpc', api_id='test_add') 
def add(x: int, y: int) -> int:
    return x + y

# 不显式设置name, 自动使用函数名作为服务id
@hook_api(api_type=['rest', 'grpc']) 
def sub(x: int, y: int) -> int:
    return x - y

# async函数自动被转换为IO任务
@hook_api(api_type=['rest', 'grpc'])
async def async_task() -> int: 
    await asyncio.sleep(10)
    return 1

# 显式作为IO任务, 非async函数显式设置为IO服务, 会被包装为线程池异步
@hook_api(api_type=['rest', 'grpc'], task_type='io')
def async_task() -> int: 
    time.sleep(10)
    return 1

# 在服务中快捷调用另外一个服务api
@hook_api(api_type=['rest', 'grpc'])
def add_all(x: list[int]) -> int: 
    return reduce(call('test_add'),x)
```


# 解析逻辑
解析分为两层, 一个是运行环境层, 一个是具体服务层

## 运行环境层
从pyproject.toml中获取运行环境信息, 或者通过PEP 723中的内容获取运行环境信息
包括
1. ray的运行node标签
2. python版本
3. 依赖信息
4. 脚本扫描文件(如果是PEP 723那样的脚本, 则默认是本文件)

## 具体服务层
根据hook_api中提供的数据获取单个服务的配置
1. 接口id(api_id)
2. 服务类型(api_type)
3. 运行类型(task_type)
4. cpu占用(task_cpu)
5. 内存占用(task_memory)

## 概念详解

