# 数据类型
| 大类      | 类型       | Grpc                          | Json                                     | python            |
| :-------- | :--------- | :---------------------------- | :--------------------------------------- | :---------------- |
| 数值      | Float      | float                         | Number                                   | float             |
| 数值      | Double     | double                        | Number                                   | float             |
| 数值      | Int32      | int32                         | Number                                   | int               |
| 数值      | Int64      | int64                         | String                                   | int               |
| 数值      | Uint32     | uint32                        | Number                                   | int               |
| 数值      | Uint64     | uint64                        | String                                   | int               |
| 数值      | Sint32     | sint32                        | Number                                   | int               |
| 数值      | Sint64     | sint64                        | String                                   | int               |
| 数值      | Fixed32    | fixed32                       | Number                                   | int               |
| 数值      | Fixed64    | fixed64                       | String                                   | int               |
| 数值      | Sfixed32   | sfixed32                      | Number                                   | int               |
| 数值      | Sfixed64   | sfixed64                      | String                                   | int               |
| 数值      | Bool       | bool                          | boolean                                  | bool              |
| 字符串    | String     | string                        | string                                   | str               |
| 二进制    | Bytes      | bytes                         | string(Base64编码)                       | bytes             |
| 嵌套类型  | List       | repeated                      | array                                    | list              |
| 嵌套类型  | Struct     | message                       | object                                   | dict              |
| wrapper   | Date       | message(sint32,sint32,sint32) | String(iso格式)                          | datetime.Date     |
| wrapper   | Datetime   | sint64                        | String(iso格式) or Number                | datetime.Datetime |
| wrapper   | DatetimeTz | message(sint64,sint32)        | String(iso格式) or object(Number,Number) | datetime.Datetime |
| dataframe | ArrowTable | 无(不在接口层使用)            | 无(不在接口层使用)                       | pyarrow.Table     |
| 别名      | Int        | sint32                        | Number                                   | int               |
| 别名      | Bigint     | sint64                        | String                                   | int               |