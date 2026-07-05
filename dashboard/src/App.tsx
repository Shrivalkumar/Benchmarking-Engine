import React, { useState, useEffect, useRef } from 'react';
import { 
  Play, 
  Terminal, 
  Upload, 
  Database, 
  TrendingUp, 
  ShieldAlert, 
  Zap, 
  RefreshCw, 
  Users, 
  Award 
} from 'lucide-react';
import { 
  ResponsiveContainer, 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend 
} from 'recharts';

interface AuthUser {
  token: string;
  username: string;
  team_name: string;
  contestant_id: number;
}

interface Standing {
  team_name: string;
  score: number;
  peak_tps: number;
  p50_latency: number;
  p90_latency: number;
  p99_latency: number;
  success_rate: number;
}

interface TelemetryTick {
  type: string;
  run_id: string;
  team_name: string;
  tps: number;
  p50: number;
  p90: number;
  p99: number;
  success_rate: number;
  total_orders: number;
  composite_score: number;
}

interface ChartDataPoint {
  time: string;
  tps: number;
  p50: number;
  p90: number;
  p99: number;
}

export default function App() {
  // Navigation & Connection configurations
  const ORCHESTRATOR_API = 'http://localhost:8010';
  const TELEMETRY_WS = 'ws://localhost:8001';

  // Auth Session States
  const [user, setUser] = useState<AuthUser | null>(() => {
    const saved = localStorage.getItem('auth_user');
    return saved ? JSON.parse(saved) : null;
  });
  const [authMode, setAuthMode] = useState<'login' | 'signup'>('login');
  const [authUsername, setAuthUsername] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authTeamName, setAuthTeamName] = useState('');
  const [authError, setAuthError] = useState('');
  const [activeTab, setActiveTab] = useState<'submission' | 'leaderboard'>('submission');

  // API state
  const [standings, setStandings] = useState<Standing[]>([]);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [activeRunTeam, setActiveRunTeam] = useState<string | null>(null);

  // Form states
  const [sourceCode, setSourceCode] = useState('');
  const [language, setLanguage] = useState<'go' | 'cpp'>('go');
  const [submissionId, setSubmissionId] = useState<number | null>(null);
  const [buildStatus, setBuildStatus] = useState<string>('');
  const [buildLogs, setBuildLogs] = useState<string>('');
  
  const [targetTps, setTargetTps] = useState(1000);
  const [concurrency, setConcurrency] = useState(20);
  const [duration, setDuration] = useState(30);

  // Benchmarking states
  const [isBuilding, setIsBuilding] = useState(false);
  const [isTesting, setIsTesting] = useState(false);
  const [testRemainingTime, setTestRemainingTime] = useState(0);

  // Real-time telemetry states
  const [liveMetrics, setLiveMetrics] = useState<TelemetryTick | null>(null);
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);

  const wsRef = useRef<WebSocket | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);

  // Fetch standings on mount
  useEffect(() => {
    fetchStandings();
    const interval = setInterval(fetchStandings, 4000);
    return () => clearInterval(interval);
  }, []);

  const fetchStandings = async () => {
    try {
      const res = await fetch(`${ORCHESTRATOR_API}/leaderboard`);
      if (res.ok) {
        const data = await res.json();
        setStandings(data);
      }
    } catch (e) {
      console.error('Failed to fetch leaderboard standings', e);
    }
  };

  // Submission Build checker
  const checkBuildStatus = async (subId: number) => {
    try {
      const res = await fetch(`${ORCHESTRATOR_API}/submissions/${subId}`);
      if (res.ok) {
        const data = await res.json();
        setBuildStatus(data.status);
        setBuildLogs(data.build_logs || 'No build logs generated yet.');
        
        if (data.status === 'built' || data.status === 'failed') {
          setIsBuilding(false);
          if (data.status === 'built') {
            alert(`🎉 Code compiled & sandboxed successfully! Submission ID: ${subId}`);
          } else {
            alert(`❌ Compilation failed. Check logs.`);
          }
          return true;
        }
      }
    } catch (e) {
      console.error(e);
    }
    return false;
  };

  // Poll build logs
  useEffect(() => {
    let pollInterval: NodeJS.Timeout;
    if (isBuilding && submissionId !== null) {
      pollInterval = setInterval(async () => {
        const done = await checkBuildStatus(submissionId);
        if (done) clearInterval(pollInterval);
      }, 2000);
    }
    return () => clearInterval(pollInterval);
  }, [isBuilding, submissionId]);

  // Handle source code compile/upload
  const handleCompileSubmit = async () => {
    if (!user) {
      alert('Please log in first');
      return;
    }
    setIsBuilding(true);
    setBuildStatus('uploading source...');
    setBuildLogs('');

    try {
      // Submit code directly using user's contestant_id
      const subRes = await fetch(`${ORCHESTRATOR_API}/submissions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${user.token}`
        },
        body: JSON.stringify({
          contestant_id: user.contestant_id,
          source_code: sourceCode,
          language: language,
        }),
      });

      if (!subRes.ok) {
        const errorData = await subRes.json().catch(() => ({ error: 'Unknown server error' }));
        if (subRes.status === 404 && errorData.error === 'Contestant not found') {
          handleLogout();
          alert('Your team session has expired or the database was reset. Please sign up or log in again.');
          return;
        }
        throw new Error(errorData.error || `Source submission failed with status ${subRes.status}`);
      }

      const subData = await subRes.json();
      setSubmissionId(subData.submission_id);
      setBuildStatus('compiling in Docker...');
    } catch (err: any) {
      setIsBuilding(false);
      setBuildStatus('failed');
      setBuildLogs('Error connecting to compiler service: ' + err.message);
    }
  };

  // Handle Authentication submit
  const handleAuthSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setAuthError('');

    const endpoint = authMode === 'login' ? '/auth/login' : '/auth/signup';
    const payload = authMode === 'login' 
      ? { username: authUsername, password: authPassword }
      : { username: authUsername, password: authPassword, team_name: authTeamName };

    try {
      const res = await fetch(`${ORCHESTRATOR_API}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || 'Authentication failed');
      }

      const userSession: AuthUser = {
        token: data.token,
        username: data.username,
        team_name: data.team_name,
        contestant_id: data.contestant_id,
      };

      localStorage.setItem('auth_user', JSON.stringify(userSession));
      setUser(userSession);
    } catch (err: any) {
      setAuthError(err.message);
    }
  };

  // Handle Logout
  const handleLogout = () => {
    localStorage.removeItem('auth_user');
    setUser(null);
    setSubmissionId(null);
    setBuildLogs('');
    setBuildStatus('');
    setActiveRunId(null);
    setLiveMetrics(null);
    setChartData([]);
  };

  // WebSocket Telemetry Connection
  const connectTelemetryWS = (runId: string) => {
    if (wsRef.current) wsRef.current.close();

    const socket = new WebSocket(TELEMETRY_WS);
    wsRef.current = socket;

    setChartData([]);

    socket.onopen = () => {
      console.log('Connected to Telemetry Websocket stream');
    };

    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'telemetry-tick' && data.run_id === runId) {
        setLiveMetrics(data);
        setChartData((prev) => {
          const updated = [...prev, {
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
            tps: data.tps,
            p50: data.p50,
            p90: data.p90,
            p99: data.p99
          }];
          return updated.slice(-30);
        });
      }
    };

    socket.onclose = () => {
      console.log('Telemetry Websocket closed');
    };
  };

  // Start stress test
  const handleStartBenchmark = async () => {
    if (!submissionId) {
      alert('Please upload and build your code first!');
      return;
    }
    
    setIsTesting(true);
    setTestRemainingTime(duration);

    try {
      const res = await fetch(`${ORCHESTRATOR_API}/benchmark/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          submission_id: submissionId,
          tps: targetTps,
          duration_seconds: duration,
          concurrency: concurrency,
        }),
      });

      if (!res.ok) {
        const error = await res.json();
        alert(error.error || 'Conflict or error starting benchmark.');
        setIsTesting(false);
        return;
      }

      const data = await res.json();
      setActiveRunId(data.benchmark_run_id);
      setActiveRunTeam(data.team_name);
      connectTelemetryWS(data.benchmark_run_id);

      // Setup UI countdown timer
      if (timerRef.current) clearInterval(timerRef.current);
      let count = duration;
      timerRef.current = setInterval(() => {
        count--;
        setTestRemainingTime(count);
        if (count <= 0) {
          clearInterval(timerRef.current!);
          setIsTesting(false);
          fetchStandings();
        }
      }, 1000);

    } catch (e: any) {
      alert('Network error starting benchmark: ' + e.message);
      setIsTesting(false);
    }
  };

  // Unauthenticated Login / Signup View (Subtle White Codeforces Theme)
  if (!user) {
    return (
      <div className="min-h-screen bg-[#eff1f3] text-[#333333] flex items-center justify-center font-sans p-6">
        <div className="w-full max-w-md bg-white border border-[#e1e4e6] rounded-md p-8 shadow-sm relative">
          <div className="absolute top-0 left-0 w-full h-1 bg-[#3b5998]" />
          <div className="text-center mb-6">
            <div className="flex justify-center items-center gap-1.5 font-bold text-[#3b5998] text-2xl tracking-tight uppercase">
              <span className="inline-block w-3.5 h-7 bg-blue-500 rounded-sm"></span>
              <span className="inline-block w-3.5 h-5 bg-red-500 rounded-sm"></span>
              <span className="inline-block w-3.5 h-9 bg-yellow-500 rounded-sm"></span>
              BENCHMARKING ENGINE
            </div>
            <p className="text-[11px] text-zinc-500 font-mono mt-1">CODEFORCES EDITION</p>
          </div>

          <form onSubmit={handleAuthSubmit} className="flex flex-col gap-4">
            {authError && (
              <div className="bg-red-50 border border-red-200 text-red-700 text-xs p-2.5 rounded">
                <div className="font-bold">Error:</div>
                <div>{authError}</div>
              </div>
            )}

            <div>
              <label className="block text-[11px] font-bold text-zinc-600 mb-1">USERNAME</label>
              <input 
                type="text" 
                value={authUsername}
                onChange={(e) => setAuthUsername(e.target.value)}
                placeholder="e.g. tourist"
                required
                className="w-full bg-white border border-[#ccc] rounded px-3 py-1.5 text-sm text-[#333] focus:outline-none focus:border-[#3b5998]"
              />
            </div>

            <div>
              <label className="block text-[11px] font-bold text-zinc-600 mb-1">PASSWORD</label>
              <input 
                type="password" 
                value={authPassword}
                onChange={(e) => setAuthPassword(e.target.value)}
                placeholder="••••••••"
                required
                className="w-full bg-white border border-[#ccc] rounded px-3 py-1.5 text-sm text-[#333] focus:outline-none focus:border-[#3b5998]"
              />
            </div>

            {authMode === 'signup' && (
              <div>
                <label className="block text-[11px] font-bold text-zinc-600 mb-1">TEAM NAME</label>
                <input 
                  type="text" 
                  value={authTeamName}
                  onChange={(e) => setAuthTeamName(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))}
                  placeholder="e.g. red_pandas"
                  required
                  className="w-full bg-white border border-[#ccc] rounded px-3 py-1.5 text-sm text-[#333] focus:outline-none focus:border-[#3b5998]"
                />
              </div>
            )}

            <button
              type="submit"
              className="w-full py-2 mt-2 font-bold text-sm bg-[#3b5998] hover:bg-[#2d4373] text-white rounded transition shadow-sm"
            >
              {authMode === 'login' ? 'Enter' : 'Register & Create Team'}
            </button>
          </form>

          <div className="text-center mt-5 border-t border-[#e1e4e6] pt-4">
            <button
              onClick={() => {
                setAuthMode(authMode === 'login' ? 'signup' : 'login');
                setAuthError('');
              }}
              className="text-xs text-[#3b5998] hover:underline"
            >
              {authMode === 'login' ? 'Register for a new account »' : 'Already registered? Log in »'}
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Authenticated Subtle White Codeforces UI
  return (
    <div className="min-h-screen bg-[#eff1f3] text-[#333333] flex flex-col font-sans select-none">
      
      {/* Codeforces Header */}
      <header className="bg-white border-b border-[#e1e4e6] px-6 py-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex flex-col md:flex-row md:items-center gap-6">
          {/* Logo block */}
          <div className="flex items-center gap-2">
            <div className="flex items-end gap-1 font-extrabold text-xl text-[#3b5998] tracking-tight uppercase">
              <span className="inline-block w-3.5 h-6 bg-blue-500 rounded-sm"></span>
              <span className="inline-block w-3.5 h-4 bg-red-500 rounded-sm"></span>
              <span className="inline-block w-3.5 h-8 bg-yellow-500 rounded-sm"></span>
              BENCHMARKING ENGINE
            </div>
          </div>

          {/* Navigation Links */}
          <nav className="flex items-center gap-1 border-t md:border-t-0 border-[#e1e4e6] pt-2 md:pt-0">
            <button
              onClick={() => setActiveTab('submission')}
              className={`px-3 py-1.5 rounded-sm text-xs font-bold transition border border-transparent ${activeTab === 'submission' ? 'border-[#e1e4e6] bg-[#eff1f3] text-[#3b5998]' : 'text-zinc-600 hover:text-[#3b5998] hover:underline'}`}
            >
              SUBMISSIONS & TESTING
            </button>
            <button
              onClick={() => setActiveTab('leaderboard')}
              className={`px-3 py-1.5 rounded-sm text-xs font-bold transition border border-transparent ${activeTab === 'leaderboard' ? 'border-[#e1e4e6] bg-[#eff1f3] text-[#3b5998]' : 'text-zinc-600 hover:text-[#3b5998] hover:underline'}`}
            >
              GLOBAL LEADERBOARD
            </button>
          </nav>
        </div>

        {/* User stats & logout */}
        <div className="flex items-center gap-4 text-xs">
          <div className="flex items-center gap-2 border border-[#e1e4e6] bg-[#f8f9fa] rounded-sm px-3 py-1.5">
            <span className="font-bold text-zinc-700">Handle:</span>
            <span className="text-[#3b5998] font-bold">{user.username}</span>
            <span className="text-zinc-400">|</span>
            <span className="font-bold text-zinc-700">Team:</span>
            <span className="text-[#0a0] font-bold uppercase">{user.team_name}</span>
          </div>

          <div className="flex items-center gap-1.5 border border-[#e1e4e6] bg-[#f8f9fa] rounded-sm px-3 py-1.5 font-bold">
            <span className={`w-2 h-2 rounded-full ${isTesting ? 'bg-red-500 animate-ping' : 'bg-green-500'}`} />
            <span className={isTesting ? 'text-red-500' : 'text-green-600'}>
              {isTesting ? 'TEST RUNNING' : 'ONLINE'}
            </span>
          </div>

          <button 
            onClick={handleLogout}
            className="px-3 py-1.5 border border-[#ccc] bg-[#eaeaea] hover:bg-[#d5d5d5] text-zinc-700 font-bold rounded-sm transition text-xs"
          >
            Logout
          </button>
        </div>
      </header>

      {/* Main Container */}
      <main className="flex-1 p-6 max-w-7xl w-full mx-auto overflow-hidden">
        {activeTab === 'submission' ? (
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 h-full overflow-y-auto">
            {/* Left Side: Upload & Run Controller */}
            <section className="lg:col-span-5 flex flex-col gap-6 pr-2 pb-6">
              
              {/* Box 1: Compile & Sandbox */}
              <div className="bg-white border border-[#e1e4e6] rounded-md shadow-sm">
                <div className="bg-[#f8f9fa] border-b border-[#e1e4e6] text-[#3b5998] font-bold text-xs uppercase py-2.5 px-4 rounded-t-md flex items-center justify-between">
                  <span className="flex items-center gap-1.5">
                    <Terminal className="w-4 h-4 text-[#3b5998]" />
                    Submit Matching Engine
                  </span>
                  <span className="text-[10px] text-zinc-500 font-mono">cgroups limit</span>
                </div>

                <div className="p-4 flex flex-col gap-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-[11px] font-bold text-zinc-600 mb-1">TEAM HANDLE</label>
                      <input 
                        type="text" 
                        value={user.team_name}
                        readOnly
                        className="w-full bg-[#f8f9fa] border border-[#e1e4e6] rounded px-3 py-1 text-sm text-zinc-500 font-mono cursor-not-allowed"
                      />
                    </div>
                    <div>
                      <label className="block text-[11px] font-bold text-zinc-600 mb-1">LANGUAGE</label>
                      <select 
                        value={language}
                        onChange={(e) => setLanguage(e.target.value as 'go' | 'cpp')}
                        className="w-full bg-white border border-[#ccc] rounded px-2.5 py-1 text-sm text-[#333] focus:outline-none focus:border-[#3b5998] cursor-pointer"
                      >
                        <option value="go">Go (Golang)</option>
                        <option value="cpp">C++ (g++)</option>
                      </select>
                    </div>
                  </div>

                  <div>
                    <label className="block text-[11px] font-bold text-zinc-600 mb-1 flex items-center justify-between">
                      <span>SOURCE CODE ({language.toUpperCase()})</span>
                      <span className="text-[10px] text-zinc-500 font-mono">EXPOSE PORT :8080</span>
                    </label>
                    <textarea 
                      value={sourceCode}
                      onChange={(e) => setSourceCode(e.target.value)}
                      className="w-full h-72 bg-white border border-[#ccc] rounded p-3 text-xs font-mono text-[#333] focus:outline-none focus:border-[#3b5998] resize-none"
                      spellCheck="false"
                      placeholder={
                        language === 'go' 
                          ? "// Paste your Go matching engine source code here...\n// Must listen on port :8080 and expose /health and /order" 
                          : "// Paste your C++ matching engine source code here...\n// Must listen on port :8080 and expose /health and /order"
                      }
                    />
                  </div>

                  <button
                    onClick={handleCompileSubmit}
                    disabled={isBuilding || isTesting}
                    className="w-full py-2 font-bold text-sm bg-[#3b5998] hover:bg-[#2d4373] disabled:bg-zinc-200 disabled:text-zinc-400 text-white rounded transition shadow-sm flex items-center justify-center gap-2"
                  >
                    {isBuilding ? (
                      <>
                        <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                        Compiling in sandbox...
                      </>
                    ) : (
                      <>
                        <Upload className="w-3.5 h-3.5" />
                        Submit & Compile Code
                      </>
                    )}
                  </button>

                  {buildStatus && (
                    <div className="bg-[#f8f9fa] border border-[#e1e4e6] rounded p-3">
                      <div className="flex items-center justify-between border-b border-[#e1e4e6] pb-1.5 mb-2 text-xs font-mono">
                        <span className="font-bold text-zinc-600">BUILD STATUS:</span>
                        <span className={`font-bold ${buildStatus === 'built' ? 'text-green-600' : buildStatus === 'failed' ? 'text-red-600' : 'text-yellow-600'}`}>
                          {buildStatus.toUpperCase()}
                        </span>
                      </div>
                      <pre className="max-h-28 overflow-y-auto text-[10px] text-zinc-600 font-mono whitespace-pre-wrap leading-tight">
                        {buildLogs || 'Awaiting compile output...'}
                      </pre>
                    </div>
                  )}
                </div>
              </div>

              {/* Box 2: Stress Test Config */}
              <div className="bg-white border border-[#e1e4e6] rounded-md shadow-sm">
                <div className="bg-[#f8f9fa] border-b border-[#e1e4e6] text-[#3b5998] font-bold text-xs uppercase py-2.5 px-4 rounded-t-md flex items-center justify-between">
                  <span className="flex items-center gap-1.5">
                    <Play className="w-4 h-4 text-[#3b5998]" />
                    Stress Test Controls
                  </span>
                  <span className="text-[10px] text-zinc-500 font-mono">Go Fleet</span>
                </div>

                <div className="p-4 flex flex-col gap-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-[11px] font-bold text-zinc-600 mb-1">TARGET TPS</label>
                      <input 
                        type="number" 
                        value={targetTps}
                        onChange={(e) => setTargetTps(Math.max(1, Number(e.target.value)))}
                        className="w-full bg-white border border-[#ccc] rounded px-3 py-1 text-sm text-[#333] focus:outline-none focus:border-[#3b5998] font-mono"
                      />
                    </div>
                    <div>
                      <label className="block text-[11px] font-bold text-zinc-600 mb-1">CONCURRENCY</label>
                      <input 
                        type="number" 
                        value={concurrency}
                        onChange={(e) => setConcurrency(Math.max(1, Number(e.target.value)))}
                        className="w-full bg-white border border-[#ccc] rounded px-3 py-1 text-sm text-[#333] focus:outline-none focus:border-[#3b5998] font-mono"
                      />
                    </div>
                  </div>

                  <div>
                    <label className="block text-[11px] font-bold text-zinc-600 mb-1">DURATION (SECONDS)</label>
                    <input 
                      type="number" 
                      value={duration}
                      onChange={(e) => setDuration(Math.max(5, Number(e.target.value)))}
                      className="w-full bg-white border border-[#ccc] rounded px-3 py-1 text-sm text-[#333] focus:outline-none focus:border-[#3b5998] font-mono"
                    />
                  </div>

                  <button
                    onClick={handleStartBenchmark}
                    disabled={isTesting || isBuilding || !submissionId}
                    className="w-full py-2 font-bold text-sm bg-[#0a0] hover:bg-[#008800] disabled:bg-zinc-200 disabled:text-zinc-400 text-white rounded transition shadow-sm flex items-center justify-center gap-1.5"
                  >
                    {isTesting ? (
                      <>
                        <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                        Testing ({testRemainingTime}s remaining)
                      </>
                    ) : (
                      <>
                        <Play className="w-3.5 h-3.5" />
                        Trigger Stress Test Run
                      </>
                    )}
                  </button>
                </div>
              </div>
            </section>

            {/* Right Side: Active Live Benchmarking Telemetry */}
            <section className="lg:col-span-7 flex flex-col gap-6 pr-2">
              {activeRunId ? (
                <div className="bg-white border border-red-500 rounded-md shadow-sm relative overflow-hidden">
                  <div className="absolute top-0 left-0 h-1 bg-red-500 transition-all duration-1000" style={{ width: `${((duration - testRemainingTime) / duration) * 100}%` }} />
                  
                  <div className="bg-[#f8f9fa] border-b border-[#e1e4e6] py-2.5 px-4 rounded-t-md flex items-center justify-between">
                    <span className="flex items-center gap-2 text-red-600 font-bold text-xs uppercase">
                      <span className="w-2.5 h-2.5 bg-red-600 rounded-full animate-ping" />
                      Live stress stream
                    </span>
                    <span className="text-[10px] text-zinc-500 font-mono">
                      RUN: {activeRunId.slice(0, 8)}
                    </span>
                  </div>

                  <div className="p-4">
                    {/* Glowing metrics */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                      <div className="bg-[#f8f9fa] border border-[#e1e4e6] rounded p-2.5 text-center">
                        <p className="text-[9px] font-bold text-zinc-500 uppercase">TPS</p>
                        <p className="text-lg font-bold text-[#0a0] font-mono">{liveMetrics?.tps || 0}</p>
                      </div>
                      <div className="bg-[#f8f9fa] border border-[#e1e4e6] rounded p-2.5 text-center">
                        <p className="text-[9px] font-bold text-zinc-500 uppercase">P99 LATENCY</p>
                        <p className="text-lg font-bold text-[#3b5998] font-mono">{liveMetrics?.p99 !== undefined ? `${liveMetrics.p99} ms` : '0.0 ms'}</p>
                      </div>
                      <div className="bg-[#f8f9fa] border border-[#e1e4e6] rounded p-2.5 text-center">
                        <p className="text-[9px] font-bold text-zinc-500 uppercase">SUCCESS</p>
                        <p className="text-lg font-bold text-[#333] font-mono">{liveMetrics?.success_rate !== undefined ? `${liveMetrics.success_rate}%` : '0.0%'}</p>
                      </div>
                      <div className="bg-[#f8f9fa] border border-[#e1e4e6] rounded p-2.5 text-center">
                        <p className="text-[9px] font-bold text-zinc-500 uppercase">SCORE</p>
                        <p className="text-lg font-bold text-yellow-600 font-mono">{liveMetrics?.composite_score || 0}</p>
                      </div>
                    </div>

                    <div className="h-64 border border-[#e1e4e6] rounded p-2 bg-white">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#f0f2f5" />
                          <XAxis dataKey="time" stroke="#71717a" fontSize={9} />
                          <YAxis yAxisId="left" stroke="#0a0" fontSize={9} />
                          <YAxis yAxisId="right" orientation="right" stroke="#3b5998" fontSize={9} />
                          <Tooltip contentStyle={{ backgroundColor: '#ffffff', borderColor: '#e1e4e6', color: '#333' }} />
                          <Legend />
                          <Line yAxisId="left" type="monotone" dataKey="tps" name="TPS" stroke="#0a0" strokeWidth={2} dot={false} activeDot={{ r: 4 }} />
                          <Line yAxisId="right" type="monotone" dataKey="p99" name="P99 Latency (ms)" stroke="#3b5998" strokeWidth={2} dot={false} activeDot={{ r: 4 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="bg-white border border-[#e1e4e6] rounded-md p-8 shadow-sm flex-1 flex flex-col items-center justify-center text-zinc-500 text-center min-h-[400px]">
                  <TrendingUp className="w-12 h-12 mb-3 text-zinc-400" />
                  <h3 className="text-sm font-bold text-zinc-700 uppercase">Awaiting Stress Run</h3>
                  <p className="text-xs text-zinc-500 max-w-sm mt-1">
                    Please upload your matching engine source code, compile it successfully, and launch the stress test to watch the live performance chart stream.
                  </p>
                </div>
              )}
            </section>
          </div>
        ) : (
          /* Leaderboard Tab (Full-width Codeforces style Table) */
          <div className="w-full max-w-6xl mx-auto h-full overflow-y-auto pr-2 pb-6">
            <div className="bg-white border border-[#e1e4e6] rounded-md shadow-sm p-6 min-h-[500px]">
              <div className="flex items-center justify-between border-b border-[#e1e4e6] pb-3 mb-6">
                <h2 className="text-md font-bold tracking-wide flex items-center gap-2 text-[#3b5998]">
                  <Award className="w-5 h-5 text-yellow-500" />
                  STANDINGS & METRIC RANKINGS
                </h2>
                <span className="text-[10px] font-mono text-zinc-500 uppercase">Composite Score = TPS / (P90 Latency + 1)</span>
              </div>

              <div className="overflow-x-auto border border-[#e1e4e6] rounded">
                {standings.length === 0 ? (
                  <div className="h-full flex flex-col items-center justify-center text-zinc-400 py-24">
                    <Database className="w-12 h-12 mb-3 text-zinc-300" />
                    <p className="text-sm font-bold uppercase">No results found</p>
                    <p className="text-xs text-zinc-500 mt-1">Compile code and trigger a benchmark run to record your score.</p>
                  </div>
                ) : (
                  <table className="w-full text-left border-collapse text-xs">
                    <thead>
                      <tr className="bg-[#f8f9fa] border-b border-[#e1e4e6] text-zinc-700 font-bold uppercase">
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6] w-16">Rank</th>
                        <th className="py-3 px-4 border-r border-[#e1e4e6]">Team Name</th>
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6]">Composite Score</th>
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6]">Peak TPS</th>
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6]">P50 Latency</th>
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6]">P90 Latency</th>
                        <th className="py-3 px-4 text-center border-r border-[#e1e4e6]">P99 Latency</th>
                        <th className="py-3 px-4 text-center">Success Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {standings.map((item, index) => {
                        const isUserTeam = item.team_name === user.team_name;
                        return (
                          <tr 
                            key={item.team_name} 
                            className={`border-b border-[#e1e4e6]/60 hover:bg-[#eff1f3]/50 transition ${isUserTeam ? 'bg-[#ffffdd] hover:bg-[#ffffcc]' : 'odd:bg-white even:bg-[#f8f9fa]'}`}
                          >
                            <td className="py-3 px-4 text-center font-bold border-r border-[#e1e4e6]">
                              {index === 0 ? (
                                <span className="text-yellow-600 font-extrabold">🥇 1</span>
                              ) : index === 1 ? (
                                <span className="text-zinc-500 font-extrabold">🥈 2</span>
                              ) : index === 2 ? (
                                <span className="text-amber-700 font-extrabold">🥉 3</span>
                              ) : (
                                index + 1
                              )}
                            </td>
                            <td className="py-3 px-4 font-bold border-r border-[#e1e4e6]">
                              <span className="text-[#3b5998] hover:underline cursor-pointer">{item.team_name}</span>
                              {isUserTeam && (
                                <span className="ml-2 bg-[#0a0]/10 text-[#0a0] border border-[#0a0]/30 text-[9px] px-1.5 py-0.5 rounded font-sans uppercase">You</span>
                              )}
                            </td>
                            <td className="py-3 px-4 text-center font-bold text-yellow-600 border-r border-[#e1e4e6]">{item.score}</td>
                            <td className="py-3 px-4 text-center font-mono text-[#0a0] border-r border-[#e1e4e6]">{item.peak_tps}</td>
                            <td className="py-3 px-4 text-center font-mono text-[#3b5998] border-r border-[#e1e4e6]">{item.p50_latency.toFixed(2)} ms</td>
                            <td className="py-3 px-4 text-center font-mono text-[#3b5998] border-r border-[#e1e4e6]">{item.p90_latency.toFixed(2)} ms</td>
                            <td className="py-3 px-4 text-center font-mono text-[#3b5998] border-r border-[#e1e4e6]">{item.p99_latency.toFixed(2)} ms</td>
                            <td className="py-3 px-4 text-center font-mono font-bold">{item.success_rate}%</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
