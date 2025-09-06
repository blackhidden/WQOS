/**
 * 进程管理Redux slice
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { ProcessStatus, DiggingProcess } from '../types/process';
import { DiggingConfig } from '../types/config';
import * as processAPI from '../services/process';

interface ProcessState {
  currentProcess: ProcessStatus | null;
  allProcesses: {
    total_processes: number;
    status: string;
    total_memory_usage: number;
    max_uptime: number;
    processes: Array<{
      pid: number;
      tag: string;
      script_type: string;
      start_time: string;
      uptime: number;
      memory_usage: number;
      cpu_usage: number;
    }>;
  } | null;
  history: DiggingProcess[];
  loading: boolean;
  error: string | null;
}

const initialState: ProcessState = {
  currentProcess: null,
  allProcesses: null,
  history: [],
  loading: false,
  error: null,
};

// 异步thunk actions
export const getProcessStatusAsync = createAsyncThunk(
  'process/getStatus',
  async () => {
    const response = await processAPI.getProcessStatus();
    return response.data;
  }
);

export const getAllProcessesStatusAsync = createAsyncThunk(
  'process/getAllStatus',
  async () => {
    const response = await processAPI.getAllProcessesStatus();
    return response.data;
  }
);

export const startProcessAsync = createAsyncThunk(
  'process/start',
  async (config: DiggingConfig) => {
    const response = await processAPI.startProcess(config);
    return response.data;
  }
);

export const startProcessFromTemplateAsync = createAsyncThunk(
  'process/startFromTemplate',
  async (params: { templateId: number; stage: number; n_jobs: number }) => {
    const response = await processAPI.startProcessFromTemplate(params.templateId, params.stage, params.n_jobs);
    return response.data;
  }
);

export const stopProcessAsync = createAsyncThunk(
  'process/stop',
  async (force: boolean = false) => {
    const response = await processAPI.stopProcess(force);
    return response.data;
  }
);

export const restartProcessAsync = createAsyncThunk(
  'process/restart',
  async (config: DiggingConfig) => {
    const response = await processAPI.restartProcess(config);
    return response.data;
  }
);

export const getProcessLogsAsync = createAsyncThunk(
  'process/getLogs',
  async ({ limit = 100, offset = 0 }: { limit?: number; offset?: number } = {}) => {
    const response = await processAPI.getProcessLogs(limit, offset);
    return response.data;
  }
);

const processSlice = createSlice({
  name: 'process',
  initialState,
  reducers: {
    clearError: (state) => {
      state.error = null;
    },
    updateProcessStatus: (state, action) => {
      state.currentProcess = action.payload;
    },
  },
  extraReducers: (builder) => {
    builder
      // Get process status
      .addCase(getProcessStatusAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(getProcessStatusAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.currentProcess = action.payload;
      })
      .addCase(getProcessStatusAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '获取进程状态失败';
      })

      // Get all processes status
      .addCase(getAllProcessesStatusAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(getAllProcessesStatusAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.allProcesses = action.payload;
      })
      .addCase(getAllProcessesStatusAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '获取所有进程状态失败';
      })

      // Start process
      .addCase(startProcessAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(startProcessAsync.fulfilled, (state, action) => {
        state.loading = false;
        // 启动成功后，状态会通过定期刷新更新
      })
      .addCase(startProcessAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '启动进程失败';
      })

      // Start process from template
      .addCase(startProcessFromTemplateAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(startProcessFromTemplateAsync.fulfilled, (state, action) => {
        state.loading = false;
        // 启动成功后，状态会通过定期刷新更新
      })
      .addCase(startProcessFromTemplateAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '从模板启动进程失败';
      })

      // Stop process
      .addCase(stopProcessAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(stopProcessAsync.fulfilled, (state, action) => {
        state.loading = false;
        // 停止成功后，状态会通过定期刷新更新
      })
      .addCase(stopProcessAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '停止进程失败';
      })

      // Restart process
      .addCase(restartProcessAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(restartProcessAsync.fulfilled, (state, action) => {
        state.loading = false;
        // 重启成功后，状态会通过定期刷新更新
      })
      .addCase(restartProcessAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '重启进程失败';
      });
  },
});

export const { clearError, updateProcessStatus } = processSlice.actions;

export default processSlice.reducer;