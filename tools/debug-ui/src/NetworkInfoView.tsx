import React from "react";
import { Navigate, NavLink, Route, Routes } from "react-router-dom";
import { CurrentPeersView } from "./CurrentPeersView";
import './NetworkInfoView.scss';
import { PeerStorageView } from "./PeerStorageView";

type NetworkInfoViewProps = {
    addr: string,
};

export const NetworkInfoView = ({ addr }: NetworkInfoViewProps) => {
    return <div className="network-info-view">
        <div className="navbar">
            <NavLink to="current" className={navLinkClassName}>Current Peers</NavLink>
            <NavLink to="peer_storage" className={navLinkClassName}>Detailed Peer Storage</NavLink>

        </div>
        <Routes>
            <Route path="" element={<Navigate to="current" />} />
            <Route path="current" element={<CurrentPeersView addr={addr} />} />
            <Route path="peer_storage" element={<PeerStorageView addr={addr} />} />
        </Routes>
    </div>;
};

function navLinkClassName({ isActive }: { isActive: boolean }) {
    return isActive ? 'nav-link active' : 'nav-link';
}
