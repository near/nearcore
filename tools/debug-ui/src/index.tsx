import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import './index.css';
import '@patternfly/react-core/dist/styles/base.css';
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { App } from './App';
import { LogVisualizer } from './log_visualizer/LogVisualizer';
import { LandingPage } from './LandingPage';

const root = ReactDOM.createRoot(document.getElementById('root') as HTMLElement);

const queryClient = new QueryClient();

const router = createBrowserRouter([
    {
        path: '/',
        element: <LandingPage />,
    },
    {
        path: '/logviz',
        element: <LogVisualizer />,
    },
    {
        path: '/:addr/*',
        element: <App />,
    },
]);

root.render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <RouterProvider router={router} />
        </QueryClientProvider>
    </React.StrictMode>
);
