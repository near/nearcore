import './EpochInfoView.scss';
import { NavLink } from 'react-router-dom';
import { Navigate, Route, Routes } from 'react-router';
import { EpochShardsView } from './EpochShardsView';
import { EpochValidatorsView } from './EpochValidatorsView';
import { RecentEpochsView } from './RecentEpochsView';

type EpochInfoViewProps = {
    addr: string;
};

export const EpochInfoView = ({ addr }: EpochInfoViewProps) => {
    return (
        <div className="epoch-info-view">
            <div className="navbar">
                <NavLink to="recent" className={navLinkClassName}>
                    Recent Epochs
                </NavLink>
                <NavLink to="validators" className={navLinkClassName}>
                    Validators
                </NavLink>
                <NavLink to="shards" className={navLinkClassName}>
                    Shard Sizes
                </NavLink>
            </div>
            <div className="content">
                <Routes>
                    <Route path="" element={<Navigate to="recent" />} />
                    <Route path="recent" element={<RecentEpochsView addr={addr} />} />
                    <Route path="validators" element={<EpochValidatorsView addr={addr} />} />
                    <Route path="shards" element={<EpochShardsView addr={addr} />} />
                </Routes>
            </div>
        </div>
    );
};

function navLinkClassName({ isActive }: { isActive: boolean }) {
    return isActive ? 'nav-link active' : 'nav-link';
}
