/**
 * @fileoverview Main application component that handles routing and authentication.
 * Sets up the application's route structure and protects authenticated routes.
 */

import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext';
import ProtectedRoute from './components/Auth/ProtectedRoute';
import Login from './components/Auth/Login';
import CallForm from './components/CallForm/CallForm';
import UploadTracker from './components/UploadTracker/UploadTracker';
import LeadTracker from './components/Lead/LeadTracker';
import SignContract from './components/SignContract/SignContract';
import OnboardingForm from './components/OnboardingForm/OnboardingForm';
import TaskGrid from './components/TaskManager/TaskGrid';
import FileManager from './components/CDN/FileManager';
import AnalyticsDashboard from './components/Analytics/AnalyticsDashboard';
import TimeSheet from './components/TimeSheet/TimeSheet';
import CDNLogin from './components/Auth/CDNLogin';
import TimeSheetLogin from './components/Auth/TimeSheetLogin';
import DemoSite from './components/DemoSite/DemoSite';
import Privacy from './components/Privacy/Privacy';
import Support from './components/Support/Support';
import VideoSummary from './components/AIReview/VideoSummary';
import Survey from './components/survey/survey';
import AdminSurvey from './components/survey/AdminDashboard';
import AIChat from './components/AIChat/AIChat';
import DesktopUpload from './components/DesktopUpload/DesktopUpload';
import SurveyLogin from './components/Auth/SurveyLogin';
import SplitAssignment from './components/Payments/SplitAssignment';
import PayeeForm from './components/Payments/PayeeForm';
import CallFormMoxy from './components/CallForm/CallForm-moxy';
import { FileMover } from './components/FileMover/file-mover';
import './global.css';

/**
 * Main application component that sets up routing and authentication
 * Wraps the entire application in AuthProvider and Router components
 * Defines protected and public routes
 * 
 * @component
 * @returns {React.ReactElement} The rendered App component
 */
function App() {
  return (
    <AuthProvider>
      <Router>
        <Routes>
          {/* Auth Routes */}
          <Route path="/login" element={<Login />} />
          <Route path="/cdn-login" element={<CDNLogin />} />
          <Route path="/timesheet-login" element={<TimeSheetLogin />} />
          <Route path="/survey-login" element={<SurveyLogin />} />
          
          {/* Public Routes */}
          <Route path="/call" element={<CallForm />} />
          <Route path="/call-moxy" element={<CallFormMoxy />} />
          <Route path="/sign-contract/:clientId" element={<SignContract />} />
          <Route path="/demo" element={<DemoSite />} />
          <Route path="/privacy" element={<Privacy />} />
          <Route path="/support" element={<Support />} />
          <Route path="/payee-form" element={<PayeeForm />} />
          
          {/* Protected Routes */}
          <Route path="/survey" element={
            <ProtectedRoute redirectTo="/survey-login">
              <Survey />
            </ProtectedRoute>
          } />
          
          <Route path="/ai-review/video-summary" element={
            <ProtectedRoute>
              <VideoSummary />
            </ProtectedRoute>
          } />

          <Route path="/cdn-manager" element={
            <ProtectedRoute>
              <FileManager />
            </ProtectedRoute>
          } />
          
          <Route path="/upload-activity" element={
            <ProtectedRoute>
              <UploadTracker />
            </ProtectedRoute>
          } />
          
          <Route path="/lead" element={
            <ProtectedRoute>
              <LeadTracker />
            </ProtectedRoute>
          } />
          
          <Route path="/onboarding" element={
            <ProtectedRoute>
              <OnboardingForm />
            </ProtectedRoute>
          } />
          
          <Route path="/tasks" element={
            <ProtectedRoute>
              <TaskGrid />
            </ProtectedRoute>
          } />
          
          <Route path="/analytics" element={
            <ProtectedRoute>
              <AnalyticsDashboard />
            </ProtectedRoute>
          } />
          
          <Route path="/analytics/dashboard" element={
            <ProtectedRoute>
              <AnalyticsDashboard />
            </ProtectedRoute>
          } />
          
          <Route path="/data" element={
            <ProtectedRoute>
              <AnalyticsDashboard />
            </ProtectedRoute>
          } />
          
          <Route path="/timesheet" element={
            <ProtectedRoute>
              <TimeSheet />
            </ProtectedRoute>
          } />
          
          <Route path="/admin/survey" element={
            <ProtectedRoute>
              <AdminSurvey />
            </ProtectedRoute>
          } />
          
          <Route path="/chat" element={
            <ProtectedRoute>
              <AIChat />
            </ProtectedRoute>
          } />
          
          <Route path="/desktop-upload" element={
            <ProtectedRoute>
              <DesktopUpload />
            </ProtectedRoute>
          } />
          
          <Route path="/filemover" element={
            <ProtectedRoute>
              <FileMover />
            </ProtectedRoute>
          } />
          
          <Route path="/payments/splits" element={
            <ProtectedRoute>
              <SplitAssignment />
            </ProtectedRoute>
          } />
          
          <Route path="/" element={
            <ProtectedRoute>
              <Navigate to="/lead" replace />
            </ProtectedRoute>
          } />
          <Route path="*" element={<Navigate to="/call" replace />} />
        </Routes>
      </Router>
    </AuthProvider>
  );
}

export default App;
