实现了简单的命令，可以用redis-cli连接本地服务器（redis-cli -p 8080），来使用"ping","get","set","del","quit"


Entry：代表db中一条数据的信息。
Storage:写入，读取数据。
Index，索引，记录一条数据的具体信息，主要是数据在磁盘中的位置。
db，db的实体。包含了db的各种操作，包括读取，写入数据。





需要拉取的依赖：github.com/tidwall/redcon
