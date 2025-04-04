
import os
import subprocess
import time
import sys

def print_header(title):
    """打印标题"""
    separator = "=" * 80
    print("\n" + separator)
    print(f"{title}".center(80))
    print(separator + "\n")

def wait_for_input(message="按回车键继续..."):
    """等待用户输入"""
    input(message)

def run_demo(demo_num):
    """运行指定的演示"""
    try:
        # 构建命令：echo 命令选项 | python demo_scenarios.py
        command = f"echo {demo_num} | python demo_scenarios.py"
        
        # 使用subprocess运行命令
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        # 实时输出结果
        output_lines = []
        for line in process.stdout:
            print(line, end='')
            output_lines.append(line)
        
        # 等待进程结束
        process.wait()
        
        return output_lines
    except Exception as e:
        print(f"运行演示时出错: {e}")
        return []

def main():
    print_header("分布式银行系统 - 并发转账问题与解决方案对比演示")
    
    # 首先确认系统是否已经启动
    print("注意: 此脚本假设您已经启动了计算机A和计算机B的银行系统")
    print("请确认以下两个程序已经在运行:")
    print("1. python start_computer_A.py")
    print("2. python start_computer_B.py")
    response = input("这两个程序是否已经在运行? (y/n): ")
    
    if response.lower() != 'y':
        print("请先运行这两个程序，然后再运行此对比演示")
        return
    
    # 显示说明
    print_header("演示说明")
    print("本演示将连续运行两个版本的并发转账演示：")
    print("1. 有问题的并发转账演示 - 展示常见并发处理问题")
    print("2. 改进版并发转账演示 - 展示问题解决方案")
    print("\n演示结束后将进行结果对比分析")
    wait_for_input()
    
    # 运行第一个演示：有问题的版本
    print_header("第1部分：有问题的并发转账演示")
    print("即将运行选项3：并发转账演示 (有问题)")
    wait_for_input()
    demo1_output = run_demo(3)
    
    # 简短暂停
    print("\n演示1完成，准备运行改进版演示...")
    time.sleep(3)
    
    # 运行第二个演示：改进版
    print_header("第2部分：改进版并发转账演示")
    print("即将运行选项7：改进版并发转账演示")
    wait_for_input()
    demo2_output = run_demo(7)
    
    # 结果对比
    print_header("结果对比分析")
    
    # 在这里可以添加更复杂的输出分析逻辑
    print("1. 有问题版本的特点:")
    print("   - 并发请求直接通过多线程同时发送")
    print("   - 使用全局锁保护资源")
    print("   - 错误恢复机制不完善")
    print("   - 没有完整的事务日志")
    print("   - 可能导致部分转账失败或余额不一致")
    
    print("\n2. 改进版本的优势:")
    print("   - 使用全局事务队列和排序")
    print("   - 实现账户级别的锁，提高并发性")
    print("   - 完善的两阶段提交和错误恢复")
    print("   - 详细的事务日志记录")
    print("   - 确保所有转账正确完成，余额保持一致")
    
    print("\n总结: 改进版本通过四个关键机制解决了并发转账问题，确保了系统的正确性和可靠性")
    print("这些改进方案易于实现，且大幅提高了系统的并发处理能力和数据一致性")
    
    print("\n对比演示完成！")

if __name__ == "__main__":
    main() 