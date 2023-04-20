import './App.scss';
import { NavLink } from 'react-router-dom';
import { Navigate, Route, Routes, useParams } from 'react-router';
import { ChainAndChunkInfoView } from './ChainAndChunkInfoView';
import { ClusterView } from './ClusterView';
import { EpochInfoView } from './EpochInfoView';
import { HeaderBar } from './HeaderBar';
import { LatestBlocksView } from './LatestBlocksView';
import { NetworkInfoView } from './NetworkInfoView';

function useNodeAddr(): string {
    const params = useParams<{ addr: string }>();
    const addr = params.addr || '127.0.0.1';
    return addr.includes(':') ? addr : addr + ':3030';
}

export const App = () => {
    const addr = useNodeAddr();
    return (
        <div className="App">
            <HeaderBar addr={addr} />
            <div className="navbar">
                <NavLink to="last_blocks" className={navLinkClassName}>
                    Latest Blocks
                </NavLink>
                <NavLink to="network_info" className={navLinkClassName}>
                    Network Info
                </NavLink>
                <NavLink to="epoch_info" className={navLinkClassName}>
                    Epoch Info
                </NavLink>
                <NavLink to="chain_and_chunk_info" className={navLinkClassName}>
                    Chain & Chunk Info
                </NavLink>
                <NavLink to="sync_info" className={navLinkClassName}>
                    Sync Info
                </NavLink>
                <NavLink to="validator_info" className={navLinkClassName}>
                    Validator Info
                </NavLink>
                <NavLink to="cluster" className={navLinkClassName}>
                    Cluster View
                </NavLink>
            </div>
            <Routes>
                <Route path="" element={<Navigate to="cluster" />} />
                <Route path="last_blocks" element={<LatestBlocksView addr={addr} />} />
                <Route path="network_info/*" element={<NetworkInfoView addr={addr} />} />
                <Route path="epoch_info/*" element={<EpochInfoView addr={addr} />} />
                <Route
                    path="chain_and_chunk_info/*"
                    element={<ChainAndChunkInfoView addr={addr} />}
                />
                <Route path="sync_info" element={<div>TODO</div>} />
                <Route path="validator_info" element={<div>TODO</div>} />
                <Route path="cluster" element={<ClusterView initialAddr={addr} />} />
            </Routes>
        </div>
    );
};

function navLinkClassName({ isActive }: { isActive: boolean }) {
    return isActive ? 'nav-link active' : 'nav-link';
}
