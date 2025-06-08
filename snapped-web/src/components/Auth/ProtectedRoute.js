import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';

const ProtectedRoute = ({ children, redirectTo = '/login' }) => {
  const { isAuthenticated } = useAuth();
  const location = useLocation();

  if (!isAuthenticated) {
    // For survey routes, pass the clientId as a query parameter
    if (location.pathname.startsWith('/survey/')) {
      const clientId = location.pathname.split('/')[2];
      return <Navigate to={`${redirectTo}?clientId=${clientId}`} replace />;
    }
    // For other routes, add the current path as a redirect parameter
    return <Navigate to={`${redirectTo}?redirect=${location.pathname}`} replace />;
  }

  return children;
};

export default ProtectedRoute; 