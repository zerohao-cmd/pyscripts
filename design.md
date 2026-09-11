# manager
## webui界面
1. 上传python scripts转换为服务
   1. 注册服务
   2. 更新服务
2. 设置服务类型
3. 设置slots
   - 最低slots
   - 最低独占slots
4. 发布服务
5. 用户权限认证
   1. 角色分类
      1. admin, 可以有多个, 有所有权限
      2. user, 每个人都是user
   2. 服务权限管理
      1. 每个服务默认所有人可查看, 可以关闭
      2. 每个服务只有一个管理者(默认是其创建者)
      3. admin或者管理者可以授权其他人管理和查看此服务

## 调度器(服务后端)
功能模块:
1. 管理所有服务状态
2. 注册新服务到worker
3. 更新worker上的服务
4. 关闭所有worker上的某服务

# scripts hook
通过指定的脚本hook功能来接入此服务
## py_hook库
功能
1. 提供pythonic的api去hook python脚本为服务
2. 提供基本的api
   1. 临时文件IO
   2. 远程KV IO

## py文件定义 
通过PEP 723中的内容次脚本代表服务的
1. python版本需求
2. python依赖信息
3. pypi server信息

# 接口
## 数据类型
| 大类     | 类型       | Grpc                          | Json                                     | python            |
| :------- | :--------- | :---------------------------- | :--------------------------------------- | :---------------- |
| 数值     | Float      | float                         | Number                                   | float             |
| 数值     | Double     | double                        | Number                                   | float             |
| 数值     | Int32      | int32                         | Number                                   | int               |
| 数值     | Int64      | int64                         | String                                   | int               |
| 数值     | Uint32     | uint32                        | Number                                   | int               |
| 数值     | Uint64     | uint64                        | String                                   | int               |
| 数值     | Sint32     | sint32                        | Number                                   | int               |
| 数值     | Sint64     | sint64                        | String                                   | int               |
| 数值     | Fixed32    | fixed32                       | Number                                   | int               |
| 数值     | Fixed64    | fixed64                       | String                                   | int               |
| 数值     | Sfixed32   | sfixed32                      | Number                                   | int               |
| 数值     | Sfixed64   | sfixed64                      | String                                   | int               |
| 数值     | Bool       | bool                          | boolean                                  | bool              |
| 字符串   | String     | string                        | string                                   | str               |
| 二进制   | Bytes      | bytes                         | string(Base64编码)                       | bytes             |
| 嵌套类型 | List       | repeated                      | array                                    | list              |
| 嵌套类型 | Struct     | message                       | object                                   | dict              |
| wrapper  | Date       | message(sint32,sint32,sint32) | String(iso格式)                          | datetime.Date     |
| wrapper  | Datetime   | sint64                        | String(iso格式) or Number                | datetime.Datetime |
| wrapper  | DatetimeTz | message(sint64,sint32)        | String(iso格式) or object(Number,Number) | datetime.Datetime |
| 别名     | Int        | sint32                        | Number                                   | int               |
| 别名     | Bigint     | sint64                        | String                                   | int               |
## grpc接口
提供grpc接口
## webhook接口
提供wehook接口

# worker组件
## 监控线程
1. 心跳服务
## 执行线程
1. 服务执行
2. 服务更新
3. 服务注册

# 服务
1. 微服务
2. 独占服务