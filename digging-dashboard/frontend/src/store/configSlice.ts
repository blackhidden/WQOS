/**
 * 配置管理Redux slice
 */

import { createSlice, createAsyncThunk, PayloadAction } from '@reduxjs/toolkit';
import { ConfigTemplate, DiggingConfig } from '../types/config';
import * as configAPI from '../services/config';

interface ConfigState {
  templates: ConfigTemplate[];
  fieldOptions: {
    regions: string[];
    universes: string[];
    instrument_types: string[];
    max_trades: string[];
    neutralizations: string[];
    modes: string[];
  };
  worldQuantOptions: any | null;
  dynamicOptions: {
    regions: any[];
    universes: any[];
    delays: any[];
    neutralizations: any[];
  };
  syncStatus: {
    is_available: boolean;
    last_sync_time: string | null;
    status: string;
  } | null;
  syncHistory: any[];
  syncing: boolean;
  loading: boolean;
  error: string | null;
  tagPreview: string | null;
  validationResult: {
    valid: boolean;
    message: string;
    tag_preview?: string;
  } | null;
}

const initialState: ConfigState = {
  templates: [],
  fieldOptions: {
    regions: [],
    universes: [],
    instrument_types: [],
    max_trades: [],
    neutralizations: [],
    modes: [],
  },
  worldQuantOptions: null,
  dynamicOptions: {
    regions: [],
    universes: [],
    delays: [],
    neutralizations: [],
  },
  syncStatus: null,
  syncHistory: [],
  syncing: false,
  loading: false,
  error: null,
  tagPreview: null,
  validationResult: null,
};

// 异步thunk actions
export const fetchConfigTemplatesAsync = createAsyncThunk(
  'config/fetchTemplates',
  async () => {
    const response = await configAPI.getConfigTemplates();
    return response.data;
  }
);

export const createConfigTemplateAsync = createAsyncThunk(
  'config/createTemplate',
  async (templateData: any) => {
    const response = await configAPI.createConfigTemplate(templateData);
    return response.data;
  }
);

export const updateConfigTemplateAsync = createAsyncThunk(
  'config/updateTemplate',
  async ({ id, data }: { id: number; data: any }) => {
    const response = await configAPI.updateConfigTemplate(id, data);
    return response.data;
  }
);

export const deleteConfigTemplateAsync = createAsyncThunk(
  'config/deleteTemplate',
  async (templateId: number) => {
    await configAPI.deleteConfigTemplate(templateId);
    return templateId;
  }
);

export const fetchFieldOptionsAsync = createAsyncThunk(
  'config/fetchFieldOptions',
  async () => {
    const response = await configAPI.getFieldOptions();
    return response.data;
  }
);

export const fetchWorldQuantOptionsAsync = createAsyncThunk(
  'config/fetchWorldQuantOptions',
  async () => {
    const response = await configAPI.getWorldQuantOptions();
    return response.data;
  }
);

export const fetchRegionsForInstrumentAsync = createAsyncThunk(
  'config/fetchRegionsForInstrument',
  async (instrumentType: string) => {
    const response = await configAPI.getRegionsForInstrument(instrumentType);
    return response.data;
  }
);

export const fetchUniversesForRegionAsync = createAsyncThunk(
  'config/fetchUniversesForRegion',
  async ({ instrumentType, region }: { instrumentType: string; region: string }) => {
    const response = await configAPI.getUniversesForRegion(instrumentType, region);
    return response.data;
  }
);

export const fetchDelaysForRegionAsync = createAsyncThunk(
  'config/fetchDelaysForRegion',
  async ({ instrumentType, region }: { instrumentType: string; region: string }) => {
    const response = await configAPI.getDelaysForRegion(instrumentType, region);
    return response.data;
  }
);

export const fetchNeutralizationsForRegionAsync = createAsyncThunk(
  'config/fetchNeutralizationsForRegion',
  async ({ instrumentType, region }: { instrumentType: string; region: string }) => {
    const response = await configAPI.getNeutralizationsForRegion(instrumentType, region);
    return response.data;
  }
);

export const generateTagAsync = createAsyncThunk(
  'config/generateTag',
  async (configData: any) => {
    const response = await configAPI.generateTag(configData);
    return response.data;
  }
);

export const validateConfigAsync = createAsyncThunk(
  'config/validateConfig',
  async (configData: DiggingConfig) => {
    const response = await configAPI.validateConfig(configData);
    return response.data;
  }
);

export const syncWorldQuantConfigAsync = createAsyncThunk(
  'config/syncWorldQuantConfig',
  async () => {
    const response = await configAPI.syncWorldQuantConfig();
    return response.data;
  }
);

export const getWorldQuantConfigStatusAsync = createAsyncThunk(
  'config/getWorldQuantConfigStatus',
  async () => {
    const response = await configAPI.getWorldQuantConfigStatus();
    return response.data;
  }
);

export const getWorldQuantSyncHistoryAsync = createAsyncThunk(
  'config/getWorldQuantSyncHistory',
  async (limit: number = 10) => {
    const response = await configAPI.getWorldQuantSyncHistory(limit);
    return response.data;
  }
);

const configSlice = createSlice({
  name: 'config',
  initialState,
  reducers: {
    clearError: (state) => {
      state.error = null;
    },
    clearTagPreview: (state) => {
      state.tagPreview = null;
    },
    clearValidationResult: (state) => {
      state.validationResult = null;
    },
    setDynamicOptions: (state, action) => {
      state.dynamicOptions = {
        ...state.dynamicOptions,
        ...action.payload
      };
    },
  },
  extraReducers: (builder) => {
    builder
      // Fetch templates
      .addCase(fetchConfigTemplatesAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(fetchConfigTemplatesAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.templates = action.payload;
      })
      .addCase(fetchConfigTemplatesAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '获取配置模板失败';
      })

      // Create template
      .addCase(createConfigTemplateAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(createConfigTemplateAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.templates.unshift(action.payload);
      })
      .addCase(createConfigTemplateAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '创建配置模板失败';
      })

      // Update template
      .addCase(updateConfigTemplateAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(updateConfigTemplateAsync.fulfilled, (state, action) => {
        state.loading = false;
        const index = state.templates.findIndex(t => t.id === action.payload.id);
        if (index !== -1) {
          state.templates[index] = action.payload;
        }
      })
      .addCase(updateConfigTemplateAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '更新配置模板失败';
      })

      // Delete template
      .addCase(deleteConfigTemplateAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(deleteConfigTemplateAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.templates = state.templates.filter(t => t.id !== action.payload);
      })
      .addCase(deleteConfigTemplateAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '删除配置模板失败';
      })

      // Fetch field options
      .addCase(fetchFieldOptionsAsync.fulfilled, (state, action) => {
        state.fieldOptions = action.payload;
      })

      // Fetch WorldQuant options
      .addCase(fetchWorldQuantOptionsAsync.fulfilled, (state, action) => {
        state.worldQuantOptions = action.payload;
      })

      // Fetch regions for instrument
      .addCase(fetchRegionsForInstrumentAsync.fulfilled, (state, action) => {
        state.dynamicOptions.regions = action.payload;
      })

      // Fetch universes for region
      .addCase(fetchUniversesForRegionAsync.fulfilled, (state, action) => {
        state.dynamicOptions.universes = action.payload;
      })

      // Fetch delays for region
      .addCase(fetchDelaysForRegionAsync.fulfilled, (state, action) => {
        state.dynamicOptions.delays = action.payload;
      })

      // Fetch neutralizations for region
      .addCase(fetchNeutralizationsForRegionAsync.fulfilled, (state, action) => {
        state.dynamicOptions.neutralizations = action.payload;
      })

      // Generate tag
      .addCase(generateTagAsync.fulfilled, (state, action) => {
        state.tagPreview = action.payload.tag;
      })

      // Validate config
      .addCase(validateConfigAsync.pending, (state) => {
        state.loading = true;
        state.error = null;
      })
      .addCase(validateConfigAsync.fulfilled, (state, action) => {
        state.loading = false;
        state.validationResult = action.payload;
      })
      .addCase(validateConfigAsync.rejected, (state, action) => {
        state.loading = false;
        state.error = action.error.message || '配置验证失败';
      })

      // Sync WorldQuant config
      .addCase(syncWorldQuantConfigAsync.pending, (state) => {
        state.syncing = true;
        state.error = null;
      })
      .addCase(syncWorldQuantConfigAsync.fulfilled, (state, action) => {
        state.syncing = false;
        // 同步成功后重新获取状态
      })
      .addCase(syncWorldQuantConfigAsync.rejected, (state, action) => {
        state.syncing = false;
        state.error = action.error.message || '同步配置失败';
      })

      // Get sync status
      .addCase(getWorldQuantConfigStatusAsync.fulfilled, (state, action) => {
        state.syncStatus = action.payload;
      })

      // Get sync history
      .addCase(getWorldQuantSyncHistoryAsync.fulfilled, (state, action) => {
        state.syncHistory = action.payload;
      });
  },
});

export const { clearError, clearTagPreview, clearValidationResult, setDynamicOptions } = configSlice.actions;

export default configSlice.reducer;