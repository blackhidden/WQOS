"""
认证相关的Pydantic模型
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, validator


class LoginRequest(BaseModel):
    """登录请求"""
    username: str
    password: str
    
    @validator('username')
    def username_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('用户名不能为空')
        return v.strip()
    
    @validator('password')
    def password_not_empty(cls, v):
        if not v:
            raise ValueError('密码不能为空')
        return v


class TokenResponse(BaseModel):
    """令牌响应"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # 过期时间（秒）
    user: "UserInfo"


class UserInfo(BaseModel):
    """用户信息"""
    id: int
    username: str
    email: Optional[str] = None
    is_active: bool
    created_at: datetime
    last_login: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class TokenData(BaseModel):
    """令牌数据"""
    username: Optional[str] = None
    user_id: Optional[int] = None


class PasswordChangeRequest(BaseModel):
    """密码修改请求"""
    current_password: str
    new_password: str
    
    @validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('新密码长度至少8位')
        return v


# 更新前向引用
TokenResponse.model_rebuild()
