/**
 * WebSocket 服务
 */

// API基础配置 - 与api.ts保持一致
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8088';

export interface WebSocketMessage {
  type: string;
  timestamp: number;
  [key: string]: any;
}

export type WebSocketMessageHandler = (message: WebSocketMessage) => void;

export class WebSocketClient {
  private ws: WebSocket | null = null;
  private url: string;
  private token: string;
  private messageHandlers: WebSocketMessageHandler[] = [];
  private reconnectInterval: number = 5000;
  private maxReconnectAttempts: number = 5;
  private reconnectAttempts: number = 0;
  private shouldReconnect: boolean = true;

  constructor(url: string, token: string) {
    this.url = url;
    this.token = token;
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        // 构建WebSocket URL（使用与API相同的配置）
        const apiUrl = new URL(API_BASE_URL);
        const protocol = apiUrl.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${apiUrl.host}${this.url}&token=${this.token}`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          this.reconnectAttempts = 0;
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const message: WebSocketMessage = JSON.parse(event.data);
            this.messageHandlers.forEach(handler => {
              try {
                handler(message);
              } catch (error) {
                console.error('WebSocket消息处理器错误:', error);
              }
            });
          } catch (error) {
            console.error('WebSocket消息解析错误:', error);
          }
        };

        this.ws.onclose = (event) => {
          this.ws = null;
          
          // 正常关闭或者不需要重连时，不要重连
          if (!this.shouldReconnect || event.code === 1000) {
            return;
          }

          if (this.reconnectAttempts < this.maxReconnectAttempts) {
            console.log(`WebSocket连接关闭，${this.reconnectInterval/1000}秒后尝试重连 (${this.reconnectAttempts + 1}/${this.maxReconnectAttempts})`);
            setTimeout(() => {
              this.reconnectAttempts++;
              this.connect().catch(() => {
                // 重连失败，忽略错误（已经有重试机制）
              });
            }, this.reconnectInterval);
          } else {
            console.error('WebSocket重连次数已达上限，停止重连');
          }
        };

        this.ws.onerror = (error) => {
          console.error('WebSocket错误:', error);
          if (this.ws?.readyState === WebSocket.CONNECTING) {
            // 连接阶段的错误，拒绝Promise
            reject(error);
          }
        };

      } catch (error) {
        reject(error);
      }
    });
  }

  disconnect() {
    this.shouldReconnect = false;
    this.messageHandlers = []; // 清空消息处理器
    if (this.ws) {
      if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) {
        this.ws.close(1000, 'Client disconnect');
      }
      this.ws = null;
    }
  }

  send(message: any) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    } else {
      console.warn('WebSocket未连接，无法发送消息');
    }
  }

  onMessage(handler: WebSocketMessageHandler) {
    this.messageHandlers.push(handler);
  }

  removeMessageHandler(handler: WebSocketMessageHandler) {
    const index = this.messageHandlers.indexOf(handler);
    if (index > -1) {
      this.messageHandlers.splice(index, 1);
    }
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }
}

/**
 * 数据集字段获取进度WebSocket客户端
 */
export class DatasetFieldsProgressWebSocket extends WebSocketClient {
  private taskId: string;

  constructor(taskId: string, token: string = 'temp-token') {
    const url = `/ws/dataset-fields-progress?task_id=${taskId}`;
    super(url, token);
    this.taskId = taskId;
  }

  getTaskId(): string {
    return this.taskId;
  }
}

/**
 * 创建数据集字段进度WebSocket客户端
 */
export function createDatasetFieldsProgressWebSocket(
  taskId: string,
  onProgress: (progress: any) => void,
  onComplete: (data: any) => void,
  onError: (error: string) => void
): DatasetFieldsProgressWebSocket {
  const ws = new DatasetFieldsProgressWebSocket(taskId);

  ws.onMessage((message) => {
    switch (message.type) {
      case 'connection':
        // WebSocket连接确认，无需日志
        break;

      case 'progress_update':
        onProgress({
          task_id: message.task_id,
          status: message.status,
          progress: message.progress,
          message: message.message,
          details: message.details
        });
        break;

      case 'task_finished':
        if (message.status === 'completed') {
          onComplete(message.data);
        } else {
          onError(`任务失败: ${message.status}`);
        }
        break;

      case 'task_not_found':
        onError('任务不存在或已被清理');
        break;

      case 'error':
        onError(message.message);
        break;

      default:
        // 忽略未知消息类型
    }
  });

  return ws;
}
