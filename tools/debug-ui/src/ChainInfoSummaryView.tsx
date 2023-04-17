import './ChainInfoSummaryView.scss';
import { useMemo } from 'react';
import { useQuery } from 'react-query';
import { fetchChainProcessingStatus, fetchFullStatus } from './api';

type ChainInfoSummaryViewProps = {
    addr: string;
};

export const ChainInfoSummaryView = ({ addr }: ChainInfoSummaryViewProps) => {
    const {
        data: fullStatus,
        error: fullStatusError,
        isLoading: fullStatusLoading,
    } = useQuery(['fullStatus', addr], () => fetchFullStatus(addr));
    const {
        data: chainProcessingInfo,
        error: chainProcessingInfoError,
        isLoading: chainProcessingInfoLoading,
    } = useQuery(['chainProcessingInfo', addr], () => fetchChainProcessingStatus(addr));
    const {
        chainInfoHead,
        chainInfoHeaderHead,
        numBlocksOrphanPool,
        numBlocksMissingChunksPool,
        numBlocksProcessing,
    } = useMemo(() => {
        let chainInfoHead = '';
        let chainInfoHeaderHead = '';
        let numBlocksOrphanPool = -1;
        let numBlocksMissingChunksPool = -1;
        let numBlocksProcessing = -1;
        if (fullStatus && chainProcessingInfo) {
            const head = fullStatus.detailed_debug_status!.current_head_status;
            chainInfoHead += head.hash;
            chainInfoHead += '@';
            chainInfoHead += head.height;
            const headerHead = fullStatus.detailed_debug_status!.current_header_head_status;
            chainInfoHeaderHead += headerHead.hash;
            chainInfoHeaderHead += '@';
            chainInfoHeaderHead += headerHead.height;
            const chainInfo = chainProcessingInfo.status_response.ChainProcessingStatus;
            numBlocksOrphanPool = chainInfo.num_orphans;
            numBlocksMissingChunksPool = chainInfo.num_blocks_missing_chunks;
            numBlocksProcessing = chainInfo.num_blocks_in_processing;
        }
        return {
            chainInfoHead,
            chainInfoHeaderHead,
            numBlocksOrphanPool,
            numBlocksMissingChunksPool,
            numBlocksProcessing,
        };
    }, [fullStatus, chainProcessingInfo]);
    if (fullStatusLoading || chainProcessingInfoLoading) {
        return <div>Loading...</div>;
    } else if (fullStatusError || chainProcessingInfoError) {
        return (
            <div className="chain-and-chunk-info-view">
                <div className="error">
                    {((fullStatusError || chainProcessingInfoError) as Error).stack}
                </div>
            </div>
        );
    } else if (!fullStatus || !chainProcessingInfo) {
        return (
            <div className="chain-and-chunk-info-view">
                <div className="error">No Data</div>
            </div>
        );
    }
    return (
        <div className="chain-info-summary-view">
            <p>
                <b>Current head: </b> {`${chainInfoHead}`}
            </p>
            <p>
                <b>Current header head: </b> {`${chainInfoHeaderHead}`}
            </p>
            <p>
                <b>Number of blocks in orphan pool: </b> {`${numBlocksOrphanPool}`}
            </p>
            <p>
                <b>Number of blocks in missing chunks pool: </b> {`${numBlocksMissingChunksPool}`}
            </p>
            <p>
                <b>Number of blocks in processing: </b> {`${numBlocksProcessing}`}
            </p>
        </div>
    );
};
