#!/usr/bin/env python3
"""
用户管理脚本
用于创建、修改、删除用户账号
"""

import sys
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from passlib.context import CryptContext

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.models import DashboardUser
from app.config import get_settings

# 密码加密
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    """加密密码"""
    return pwd_context.hash(password)

def get_db_session():
    """获取数据库会话"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def create_user(username: str, password: str, email: str = None):
    """创建新用户"""
    db = get_db_session()
    try:
        # 检查用户是否已存在
        existing_user = db.query(DashboardUser).filter(DashboardUser.username == username).first()
        if existing_user:
            print(f"❌ 用户 '{username}' 已存在")
            return False
        
        # 创建新用户
        new_user = DashboardUser(
            username=username,
            password_hash=get_password_hash(password),
            email=email or f"{username}@worldquant.local",
            is_active=True
        )
        
        db.add(new_user)
        db.commit()
        print(f"✅ 用户 '{username}' 创建成功")
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ 创建用户失败: {e}")
        return False
    finally:
        db.close()

def update_password(username: str, new_password: str):
    """更新用户密码"""
    db = get_db_session()
    try:
        user = db.query(DashboardUser).filter(DashboardUser.username == username).first()
        if not user:
            print(f"❌ 用户 '{username}' 不存在")
            return False
        
        user.password_hash = get_password_hash(new_password)
        db.commit()
        print(f"✅ 用户 '{username}' 密码更新成功")
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ 更新密码失败: {e}")
        return False
    finally:
        db.close()

def list_users():
    """列出所有用户"""
    db = get_db_session()
    try:
        users = db.query(DashboardUser).all()
        if not users:
            print("📝 暂无用户")
            return
        
        print("👥 用户列表:")
        print("-" * 50)
        for user in users:
            status = "🟢 激活" if user.is_active else "🔴 禁用"
            print(f"ID: {user.id:2d} | 用户名: {user.username:15s} | 邮箱: {user.email:25s} | 状态: {status}")
        
    except Exception as e:
        print(f"❌ 获取用户列表失败: {e}")
    finally:
        db.close()

def delete_user(username: str):
    """删除用户"""
    db = get_db_session()
    try:
        user = db.query(DashboardUser).filter(DashboardUser.username == username).first()
        if not user:
            print(f"❌ 用户 '{username}' 不存在")
            return False
        
        # 确认删除
        confirm = input(f"⚠️  确定要删除用户 '{username}' 吗？(y/N): ")
        if confirm.lower() != 'y':
            print("❌ 操作已取消")
            return False
        
        db.delete(user)
        db.commit()
        print(f"✅ 用户 '{username}' 删除成功")
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ 删除用户失败: {e}")
        return False
    finally:
        db.close()

def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("🔧 WorldQuant 挖掘面板用户管理工具")
        print("")
        print("用法:")
        print("  python manage_users.py list                           # 列出所有用户")
        print("  python manage_users.py create <username> <password>   # 创建用户")
        print("  python manage_users.py password <username> <password> # 修改密码")
        print("  python manage_users.py delete <username>              # 删除用户")
        print("")
        print("示例:")
        print("  python manage_users.py create newuser MyPassword123")
        print("  python manage_users.py password admin NewPassword456")
        return
    
    command = sys.argv[1]
    
    if command == "list":
        list_users()
    
    elif command == "create":
        if len(sys.argv) != 4:
            print("❌ 用法: python manage_users.py create <username> <password>")
            return
        username, password = sys.argv[2], sys.argv[3]
        create_user(username, password)
    
    elif command == "password":
        if len(sys.argv) != 4:
            print("❌ 用法: python manage_users.py password <username> <new_password>")
            return
        username, new_password = sys.argv[2], sys.argv[3]
        update_password(username, new_password)
    
    elif command == "delete":
        if len(sys.argv) != 3:
            print("❌ 用法: python manage_users.py delete <username>")
            return
        username = sys.argv[2]
        delete_user(username)
    
    else:
        print(f"❌ 未知命令: {command}")
        print("支持的命令: list, create, password, delete")

if __name__ == "__main__":
    main()
