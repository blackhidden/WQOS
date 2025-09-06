#!/usr/bin/env python3
"""
统一会话管理命令行工具
作者：e.e.
微信：Enkidu_lin
日期：2025.08.24

提供便捷的会话管理命令：
- 查看会话状态
- 刷新会话
- 清除会话
- 测试会话
"""

import sys
import argparse
from datetime import datetime
import os

# 添加src目录到路径
sys.path.append(os.path.dirname(__file__))

from session_client import get_session_info


def cmd_status():
    """显示会话状态"""
    print("📊 统一会话管理器状态")
    print("=" * 40)
    
    try:
        info = get_session_info()
        
        if info['status'] == 'no_session':
            print("❌ 当前无活跃会话")
            return
        
        print(f"状态: {'🟢 活跃' if info['status'] == 'active' else '🔴 非活跃'}")
        print(f"开始时间: {info['start_time']}")
        print(f"过期时间: {info['expires_at']}")
        print(f"剩余时间: {info['time_left_minutes']:.0f} 分钟")
        print(f"是否过期: {'❌ 是' if info['is_expired'] else '✅ 否'}")
        
        if info.get('user_info'):
            print(f"用户信息: {info['user_info']}")
            
    except Exception as e:
        print(f"❌ 获取会话状态失败: {e}")


def cmd_refresh():
    """刷新会话"""
    print("🔄 刷新会话...")
    
    try:
        success = refresh_session()
        if success:
            print("✅ 会话刷新成功")
            cmd_status()  # 显示新的会话状态
        else:
            print("❌ 会话刷新失败")
    except Exception as e:
        print(f"❌ 会话刷新异常: {e}")


def cmd_test():
    """测试会话是否有效"""
    print("🧪 测试会话有效性...")
    
    try:
        session = get_session()
        
        # 测试API调用
        response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
        
        if response.status_code == 200:
            print("✅ 会话有效，API调用成功")
            user_data = response.json()
            print(f"用户ID: {user_data.get('id', 'N/A')}")
            print(f"用户名: {user_data.get('username', 'N/A')}")
        else:
            print(f"❌ API调用失败: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ 会话测试失败: {e}")


def cmd_clear():
    """清除当前会话"""
    print("🗑️ 清除当前会话...")
    
    try:
        invalidate_session()
        print("✅ 会话已清除")
    except Exception as e:
        print(f"❌ 清除会话失败: {e}")


def cmd_info():
    """显示详细的会话信息"""
    print("📋 详细会话信息")
    print("=" * 40)
    
    try:
        manager = get_session_manager()
        
        # 基本状态
        cmd_status()
        
        print("\n🔧 管理器配置:")
        print(f"会话持续时间: {manager.session_duration // 3600} 小时")
        print(f"刷新阈值: {manager.refresh_threshold // 60} 分钟")
        print(f"使用数据库: {'✅' if manager.use_database else '❌'}")
        print(f"文件备份: {'✅' if manager.fallback_to_file else '❌'}")
        
        print(f"\n💾 存储位置:")
        print(f"会话文件: {manager.session_file}")
        print(f"Cookie文件: {manager.cookie_file}")
        
        # 检查文件是否存在
        session_file_exists = manager.session_file.exists()
        cookie_file_exists = manager.cookie_file.exists()
        print(f"会话文件存在: {'✅' if session_file_exists else '❌'}")
        print(f"Cookie文件存在: {'✅' if cookie_file_exists else '❌'}")
        
        # 数据库状态
        if manager.db_manager:
            try:
                db_session_data = manager.db_manager.get_config('unified_session_data')
                print(f"数据库会话数据: {'✅ 存在' if db_session_data else '❌ 不存在'}")
            except Exception as e:
                print(f"数据库会话数据: ❌ 检查失败 ({e})")
        else:
            print("数据库会话数据: ❌ 数据库未连接")
            
    except Exception as e:
        print(f"❌ 获取详细信息失败: {e}")


def cmd_monitor():
    """监控会话状态（持续显示）"""
    import time
    
    print("👀 会话状态监控 (按 Ctrl+C 退出)")
    print("=" * 40)
    
    try:
        while True:
            # 清屏并显示当前时间
            print(f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 显示会话状态
            info = get_session_info()
            if info['status'] != 'no_session':
                status_icon = "🟢" if not info['is_expired'] else "🔴"
                print(f"{status_icon} 状态: {info['status']}")
                print(f"⏱️ 剩余: {info['time_left_minutes']:.0f} 分钟")
                
                # 警告即将过期
                if info['time_left_minutes'] < 30:
                    print("⚠️ 会话即将过期！")
            else:
                print("❌ 无活跃会话")
            
            time.sleep(30)  # 每30秒更新一次
            
    except KeyboardInterrupt:
        print("\n👋 监控已停止")


def cmd_benchmark():
    """性能基准测试"""
    import time
    
    print("📊 会话管理性能基准测试")
    print("=" * 40)
    
    # 测试获取会话的性能
    print("🧪 测试获取会话性能...")
    start_time = time.time()
    
    sessions = []
    for i in range(20):
        session = get_session()
        sessions.append(session)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"✅ 20次获取会话耗时: {total_time:.3f}秒")
    print(f"📈 平均每次: {total_time/20*1000:.1f}毫秒")
    
    # 检查是否复用同一会话
    unique_sessions = len(set(id(s) for s in sessions))
    print(f"🔄 会话复用: {unique_sessions == 1} (唯一会话数: {unique_sessions})")
    
    # 测试API调用性能
    print("\n🧪 测试API调用性能...")
    session = get_session()
    
    api_times = []
    for i in range(5):
        start = time.time()
        response = session.get('https://api.worldquantbrain.com/users/self', timeout=10)
        end = time.time()
        
        if response.status_code == 200:
            api_times.append(end - start)
        else:
            print(f"❌ API调用失败: HTTP {response.status_code}")
    
    if api_times:
        avg_api_time = sum(api_times) / len(api_times)
        print(f"✅ API调用平均响应时间: {avg_api_time:.3f}秒")
        print(f"📊 API调用范围: {min(api_times):.3f}s - {max(api_times):.3f}s")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='统一会话管理命令行工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  %(prog)s status          # 查看会话状态
  %(prog)s refresh         # 刷新会话
  %(prog)s test            # 测试会话有效性
  %(prog)s clear           # 清除当前会话
  %(prog)s info            # 显示详细信息
  %(prog)s monitor         # 持续监控会话状态
  %(prog)s benchmark       # 性能基准测试
        """
    )
    
    parser.add_argument(
        'command',
        choices=['status', 'refresh', 'test', 'clear', 'info', 'monitor', 'benchmark'],
        help='要执行的命令'
    )
    
    args = parser.parse_args()
    
    # 根据命令执行对应函数
    commands = {
        'status': cmd_status,
        'refresh': cmd_refresh,
        'test': cmd_test,
        'clear': cmd_clear,
        'info': cmd_info,
        'monitor': cmd_monitor,
        'benchmark': cmd_benchmark,
    }
    
    try:
        commands[args.command]()
    except KeyboardInterrupt:
        print("\n👋 操作已取消")
    except Exception as e:
        print(f"❌ 命令执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
