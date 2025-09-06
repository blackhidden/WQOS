#!/usr/bin/env python3
"""
数据库初始化脚本
"""

import asyncio
from sqlalchemy.orm import Session
from app.db.database import SessionLocal, create_tables
from app.db.models import DashboardUser, DiggingConfigTemplate
from app.core.auth import get_password_hash
import json

def init_database():
    """初始化数据库"""
    print("🔄 正在初始化数据库...")
    
    # 创建表
    create_tables()
    print("✅ 数据库表创建完成")
    
    # 创建会话
    db = SessionLocal()
    
    try:
        # 检查是否已有用户
        existing_user = db.query(DashboardUser).first()
        if not existing_user:
            # 创建默认管理员用户
            admin_user = DashboardUser(
                username="admin",
                password_hash=get_password_hash("admin123"),
                email="admin@worldquant.local",
                is_active=True
            )
            db.add(admin_user)
            db.commit()
            db.refresh(admin_user)
            print(f"✅ 创建默认用户: {admin_user.username}")
            
            # 创建示例配置模板
            default_template = DiggingConfigTemplate(
                template_name="默认美股配置",
                description="默认的美股挖掘配置模板",
                use_recommended_fields=False,
                region="USA",
                universe="TOP3000",
                delay=1,
                instrument_type="EQUITY",
                max_trade="OFF",
                dataset_id="fundamental6",
                created_by=admin_user.id
            )
            db.add(default_template)
            
            recommended_template = DiggingConfigTemplate(
                template_name="推荐字段配置",
                description="使用推荐字段的配置模板",
                use_recommended_fields=True,
                region="USA",
                universe="TOP3000",
                delay=1,
                instrument_type="EQUITY",
                max_trade="OFF",
                recommended_name="analyst11",
                recommended_fields=json.dumps(["close", "volume", "market_cap", "pe_ratio"]),
                created_by=admin_user.id
            )
            db.add(recommended_template)
            
            db.commit()
            print("✅ 创建示例配置模板")
        else:
            print("✅ 数据库已有数据，跳过初始化")
    
    except Exception as e:
        print(f"❌ 数据库初始化失败: {e}")
        db.rollback()
        raise
    
    finally:
        db.close()

def main():
    """主函数"""
    print("🚀 WorldQuant 挖掘控制面板数据库初始化")
    print("=" * 50)
    
    try:
        init_database()
        print("=" * 50)
        print("✅ 数据库初始化完成")
        print("🔑 默认登录信息:")
        print("   用户名: admin")
        print("   密码: admin123")
        print("⚠️  请在生产环境中修改默认密码")
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
