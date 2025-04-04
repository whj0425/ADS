# 分布式银行系统 (ADS - Autonomous Distributed System)

这是一个分布式银行系统的实现，支持分布式事务处理、容错机制和并发控制。

## 项目结构

本项目使用了更加清晰的目录结构组织代码。详细的文件组织请参考 [文件组织说明](docs/FILE_ORGANIZATION.md)。

主要目录:
- `src/` - 核心系统组件
- `tests/` - 测试相关文件
- `demos/` - 演示脚本
- `data/` - 配置和数据文件 
- `docs/` - 文档文件

## 运行说明

### 本地运行

在单机环境下启动系统:

```
python src/start_system.py
```

### 分布式运行

在分布式环境中运行系统:

1. 在计算机A上运行:
```
python demos/start_computer_A.py
```

2. 在计算机B上运行:
```
python demos/start_computer_B.py
```

## 演示场景

可以运行以下演示场景测试系统功能:

```
python demos/demo_scenarios.py
```

## 文档

更多详细信息请参考 `docs/` 目录下的文档文件:

- [项目需求](docs/ADSProjectRequirements.md)
- [系统设计](docs/DBSPlan.md)
- [并发控制说明](docs/CONCURRENT_DEMO_README.md) 