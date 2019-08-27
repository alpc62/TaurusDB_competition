### 说明
* 接口：src/include
* 参考demo：example

### build - 在代码根目录执行
* mkdir build
* cd build
* cmake ..
* make

### run - 在build目录执行
* 启动存储节点：example/kv_store/example_store port data_dir [clear]
* 测试计算节点: test/tester kv_number thread_number so_path store_side_url

### 其他
1 参赛者可以使用其他的通信协议，但需要自行继承到cmake编译配置中；
2 每轮测试前（包括一次测试中间的recovery前），计算节点（对应kv_service）中的所有数据将被清空，不允许在kv_service中保存任何持久化信息，也不允许利用系统缓存信息；
3 存储节点的输出文件名必须为kv_store，必须实现示例demo中的console接口；

