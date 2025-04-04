# 文件组织结构

本项目已经进行了重新组织，以提高代码库的清晰度和可维护性。以下是新的目录结构：

## 目录结构

```
ADS/
├── src/             # 核心系统组件
│   ├── account_node.py           # 账户节点实现
│   ├── transaction_coordinator.py # 事务协调器实现
│   ├── client.py                 # 客户端实现
│   ├── start_system.py           # 系统启动脚本
│   └── __pycache__/              # Python缓存文件
├── tests/           # 测试相关文件
│   ├── compare_demos.py          # 演示比较
│   ├── failure_test.py           # 失败测试
│   └── concurrent_test.py        # 并发测试
├── demos/           # 演示脚本
│   ├── demo_scenarios.py         # 演示场景
│   ├── improved_concurrent_demo.py # 改进的并发演示
│   ├── start_computer_A.py       # 计算机A启动脚本
│   └── start_computer_B.py       # 计算机B启动脚本
├── data/          # 配置和数据文件
│   ├── coordinator_data.json     # 协调器数据
│   ├── a1_data.json              # 账户a1数据
│   ├── a1b_data.json             # 账户a1b数据
│   └── src_backups/              # 源目录中的JSON备份文件
├── docs/            # 文档文件
│   ├── CONCURRENT_DEMO_README.md
│   ├── 并发控制失败.md
│   ├── README_DEMO.md
│   ├── DBSPlan.md
│   ├── DBS.md
│   ├── ADSProjectRequirements.md
│   └── FILE_ORGANIZATION.md      # 本文档
└── .git/            # Git版本控制目录
```

## 路径引用更改

所有引用JSON文件的代码都已更新，使用`data/`目录作为数据文件的存储位置。这包括：

1. `src/transaction_coordinator.py` - 更新了`self.data_file`路径
2. `src/account_node.py` - 更新了`self.data_file`路径
3. `demos/start_computer_A.py` - 更新了配置创建函数中的路径
4. `demos/start_computer_B.py` - 更新了账户数据创建函数中的路径
5. `demos/improved_concurrent_demo.py` - 更新了`self.data_file`路径

## 注意事项

- JSON文件是由脚本动态生成的，移动后确保了更加清晰的文件组织结构
- `src_backups`目录包含从原`src`目录移动的JSON文件备份
- 所有代码文件引用的路径都已更新，确保系统正常运行 