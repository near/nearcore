import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import { createBrowserRouter, Navigate, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from 'react-query';
import { App } from './App';
import 'react-tooltip/dist/react-tooltip.css';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);

const queryClient = new QueryClient();

const router = createBrowserRouter([{
  // Root path redirects to the current host - this is useful if we serve the app from the node
  // itself.
  path: '/',
  element: <Navigate to={'/' + window.location.host} />
}, {
  path: '/:addr/*',
  element: <App />
}]);

root.render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>
);
