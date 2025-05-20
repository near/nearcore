import './ChainAndChunkInfoView.scss';
import { NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { BlocksView } from './BlocksView';
import { ChainInfoSummaryView } from './ChainInfoSummaryView';
import { FloatingChunksView } from './FloatingChunksView';

type ChainAndChunkInfoViewProps = {
    addr: string;
};

export const ChainAndChunkInfoView = ({ addr }: ChainAndChunkInfoViewProps) => {
    return (
        <div className="chain-and-chunk-info-view">
            <div className="navbar">
                <NavLink to="chain_info_summary" className={navLinkClassName}>
                    Chain Info Summary
                </NavLink>
                <NavLink to="floating_chunks" className={navLinkClassName}>
                    Floating Chunks
                </NavLink>
                <NavLink to="blocks" className={navLinkClassName}>
                    Blocks
                </NavLink>
            </div>
            <Routes>
                <Route path="" element={<Navigate to="chain_info_summary" />} />
                <Route path="chain_info_summary" element={<ChainInfoSummaryView addr={addr} />} />
                <Route path="floating_chunks" element={<FloatingChunksView addr={addr} />} />
                <Route path="blocks" element={<BlocksView addr={addr} />} />
            </Routes>
        </div>
    );
};

function navLinkClassName({ isActive }: { isActive: boolean }) {
    return isActive ? 'nav-link active' : 'nav-link';
}
