<p>本项目是一个用go写的迁移数据到分库分表的demo。</p>

## 特点

1. 使用go协程特性
1. 协程执行错误时记录日志
1. 程序执行时进度条显示
1. 不同的机器可以通过配置分批查询参数和批量插入参数达到最佳性能

## 使用方式：

```go
//创建数据表
go run main.go -mode=tables -ac=create

//删除数据表
go run main.go -mode=tables -ac=drop

//迁移数据
go run main.go -mode=migrate
```
