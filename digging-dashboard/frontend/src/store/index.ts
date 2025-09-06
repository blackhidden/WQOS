/**
 * Redux Store 配置
 */

import { configureStore } from '@reduxjs/toolkit';
import authSlice from './authSlice';
import configSlice from './configSlice';
import processSlice from './processSlice';

export const store = configureStore({
  reducer: {
    auth: authSlice,
    config: configSlice,
    process: processSlice,
  },
  middleware: (getDefaultMiddleware) =>
    getDefaultMiddleware({
      serializableCheck: {
        ignoredActions: ['persist/PERSIST'],
      },
    }),
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
