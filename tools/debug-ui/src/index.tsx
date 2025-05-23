import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import './index.css';
import 'react-tooltip/dist/react-tooltip.css';
import '@patternfly/react-core/dist/styles/base.css';
import { createBrowserRouter, Navigate, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App } from './App';
import { LogVisualizer } from './log_visualizer/LogVisualizer';
import { LandingPage } from './LandingPage';

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement);

const queryClient = new QueryClient();

const router = createBrowserRouter([
    {
        index: true,
        element: <LandingPage />,
    },
    {
        path: 'logviz',
        element: <LogVisualizer />,
    },
    {
        path: ':addr',
        children: [
            {
                index: true,
                element: <Navigate to="cluster" />,
            },
            {
                path: '*',
                element: <App />,
            },
        ],
    },
]);

root.render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <RouterProvider router={router} />
        </QueryClientProvider>
    </React.StrictMode>
);
