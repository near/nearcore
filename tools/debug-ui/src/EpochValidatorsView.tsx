import { partition } from 'lodash';
import { useQuery } from '@tanstack/react-query';
import { useId, useState } from 'react';
import { Tooltip } from 'react-tooltip';
import { EpochInfoView, ValidatorKickoutReason, fetchEpochInfo } from './api';
import './EpochValidatorsView.scss';

interface ProducedAndExpected {
    produced: number;
    expected: number;
}

interface BlockProducer {
    kind: 'BlockProducer';
}

interface ChunkProducer {
    kind: 'ChunkProducer';
    shards: number[];
}

interface ChunkValidator {
    kind: 'ChunkValidator';
}

type ValidatorRole = BlockProducer | ChunkProducer | ChunkValidator;

interface CurrentValidatorInfo {
    stake: number;
    shards: number[];
    blocks: ProducedAndExpected;
    chunks: ProducedAndExpected;
    endorsements: ProducedAndExpected;
}

interface NextValidatorInfo {
    stake: number;
    shards: number[];
}

interface ValidatorInfo {
    accountId: string;
    current: CurrentValidatorInfo | null;
    next: NextValidatorInfo | null;
    proposalStake: number | null;
    kickoutReason: ValidatorKickoutReason | null;
    roles: ValidatorRole[][];
}

class Validators {
    validators: Map<string, ValidatorInfo> = new Map();

    constructor(private numEpochs: number) {}

    validator(accountId: string): ValidatorInfo {
        if (this.validators.has(accountId)) {
            return this.validators.get(accountId)!;
        }
        const roles = [] as ValidatorRole[][];
        for (let i = 0; i < this.numEpochs; i++) {
            roles.push([]);
        }
        this.validators.set(accountId, {
            accountId,
            current: null,
            next: null,
            proposalStake: null,
            kickoutReason: null,
            roles,
        });
        return this.validators.get(accountId)!;
    }

    addValidatorRole(accountId: string, epochIndex: number, role: ValidatorRole) {
        const validator = this.validator(accountId);
        validator.roles[epochIndex].push(role);
        validator.roles[epochIndex].sort((a, b) => {
            return a.kind.localeCompare(b.kind);
        });
    }

    sorted(): ValidatorInfo[] {
        const validators = [...this.validators.values()];
        function sortingKey(info: ValidatorInfo) {
            if (info.current !== null) {
                return [0, -info.current.stake];
            }
            if (info.next !== null) {
                return [1, -info.next.stake];
            }
            if (info.proposalStake !== null) {
                return [2, -info.proposalStake];
            }
            return [3, 0];
        }
        validators.sort((a, b) => {
            const [ax, ay] = sortingKey(a);
            const [bx, by] = sortingKey(b);
            if (ax == bx) {
                return ay - by;
            }
            return ax - bx;
        });
        return validators;
    }
}

interface ProcessedEpochData {
    validators: Validators;
    maxStake: number;
    totalStake: number;
    maxExpectedBlocks: number;
    maxExpectedChunks: number;
    maxExpectedEndorsements: number;
}

function processEpochData(epochData: any): ProcessedEpochData {
    let maxStake = 0,
        totalStake = 0,
        maxExpectedBlocks = 0,
        maxExpectedChunks = 0,
        maxExpectedEndorsements = 0;
    const epochs = epochData.status_response.EpochInfo;
    const validators = new Validators(epochs.length);
    const currentValidatorInfo = epochData.status_response.EpochInfo[1].validator_info;

    for (const validatorInfo of currentValidatorInfo.current_validators) {
        const validator = validators.validator(validatorInfo.account_id);
        const stake = parseFloat(validatorInfo.stake);
        validator.current = {
            stake,
            shards: validatorInfo.shards,
            blocks: {
                produced: validatorInfo.num_produced_blocks,
                expected: validatorInfo.num_expected_blocks,
            },
            chunks: {
                produced: validatorInfo.num_produced_chunks,
                expected: validatorInfo.num_expected_chunks,
            },
            endorsements: {
                produced: validatorInfo.num_produced_endorsements,
                expected: validatorInfo.num_expected_endorsements,
            },
        };
        maxStake = Math.max(maxStake, stake);
        totalStake += stake;
        maxExpectedBlocks = Math.max(maxExpectedBlocks, validatorInfo.num_expected_blocks);
        maxExpectedChunks = Math.max(maxExpectedChunks, validatorInfo.num_expected_chunks);
        maxExpectedEndorsements = Math.max(
            maxExpectedEndorsements,
            validatorInfo.num_expected_endorsements
        );
    }

    for (const validatorInfo of currentValidatorInfo.next_validators) {
        const validator = validators.validator(validatorInfo.account_id);
        validator.next = {
            stake: parseFloat(validatorInfo.stake),
            shards: validatorInfo.shards,
        };
        if (validatorInfo.shards.length > 0) {
            validators.addValidatorRole(validator.accountId, 0, {
                kind: 'ChunkProducer',
                shards: validatorInfo.shards,
            });
        }
        validators.addValidatorRole(validator.accountId, 0, {
            kind: 'ChunkValidator',
        });
    }

    for (const proposal of currentValidatorInfo.current_proposals) {
        const validator = validators.validator(proposal.account_id);
        validator.proposalStake = parseFloat(proposal.stake);
    }

    for (const kickout of currentValidatorInfo.prev_epoch_kickout) {
        const validator = validators.validator(kickout.account_id);
        validator.kickoutReason = kickout.reason;
    }

    epochs.forEach((epochInfo: any, index: number) => {
        for (const blockProducer of epochInfo.block_producers) {
            validators.addValidatorRole(blockProducer.account_id, index, { kind: 'BlockProducer' });
        }
        if (epochInfo.validator_info != null) {
            for (const validator of epochInfo.validator_info.current_validators) {
                if (validator.num_expected_chunks > 0) {
                    validators.addValidatorRole(validator.account_id, index, {
                        kind: 'ChunkProducer',
                        shards: validator.shards,
                    });
                }
                if (validator.num_expected_endorsements > 0) {
                    validators.addValidatorRole(validator.account_id, index, {
                        kind: 'ChunkValidator',
                    });
                }
            }
        }
    });

    return {
        validators,
        maxStake,
        totalStake,
        maxExpectedBlocks,
        maxExpectedChunks,
        maxExpectedEndorsements,
    };
}

type EpochValidatorViewProps = {
    addr: string;
};

export const EpochValidatorsView = ({ addr }: EpochValidatorViewProps) => {
    const [enteredEpochId, setEnteredEpochId] = useState<string>('');
    const [currentEpochId, setCurrentEpochId] = useState<string | null>(null);
    const [validators, setValidators] = useState<Validators | null>(null);
    const [maxStake, setMaxStake] = useState<number>(0);
    const [totalStake, setTotalStake] = useState<number>(0);
    const [maxExpectedBlocks, setMaxExpectedBlocks] = useState<number>(0);
    const [maxExpectedChunks, setMaxExpectedChunks] = useState<number>(0);
    const [maxExpectedEndorsements, setMaxExpectedEndorsements] = useState<number>(0);

    const { data: epochData, error: epochError, isLoading: epochIsLoading, isFetching } = useQuery(
        ['epochInfo', addr, currentEpochId],
        () => fetchEpochInfo(addr, currentEpochId),
        {
            onSuccess: (data) => {
                const { 
                    validators, maxStake, totalStake, 
                    maxExpectedBlocks, maxExpectedChunks, maxExpectedEndorsements 
                } = processEpochData(data);
                setValidators(validators);
                setMaxStake(maxStake);
                setTotalStake(totalStake);
                setMaxExpectedBlocks(maxExpectedBlocks);
                setMaxExpectedChunks(maxExpectedChunks);
                setMaxExpectedEndorsements(maxExpectedEndorsements);
            },
            keepPreviousData: true
        }
    );

    if (epochIsLoading) {	
        return <div>Loading...</div>;	
    }
    if (epochError) {
        return <div className="error">{(epochError as Error).stack}</div>;
    }

    const handleNavigateEpoch = async (direction: 'left' | 'right') => {
        if (!epochData?.status_response.EpochInfo) return;
        
        const epochs = epochData.status_response.EpochInfo;
        const currentIndex = 1;
        const targetIndex = direction === 'left' ? currentIndex - 1 : currentIndex + 1;
        
        if (targetIndex < 0 || targetIndex >= epochs.length) {
            alert('No more epochs available in that direction');
            return;
        }

        const targetEpochId = epochs[targetIndex].epoch_id;
        setCurrentEpochId(targetEpochId);
        setEnteredEpochId(targetEpochId);
    };

    const handleButtonClick = async () => {
        try {
            setCurrentEpochId(enteredEpochId || null);
        } catch (error) {
            console.error('Error fetching epoch data:', error);
            alert('Failed to fetch epoch data');
        }
    };

    const handleEpochIdInput = (event: React.ChangeEvent<HTMLInputElement>) => {
        setEnteredEpochId(event.target.value);
    };

    function renderTableHeaders(epochData: any): JSX.Element {
        return (
            <thead>
                <tr>
                    <th></th>
                    <th colSpan={3}>Next Epoch {epochData?.status_response.EpochInfo[0].epoch_height}</th>
                    <th colSpan={5}>Current Epoch {epochData?.status_response.EpochInfo[1].epoch_height}</th>
                    <th colSpan={1 + (epochData?.status_response.EpochInfo.length || 0) - 2}>Past Epochs</th>
                </tr>
                <tr>
                    <th>Validator</th>
                    <th className="small-text">Roles (shards)</th>
                    <th>Stake</th>
                    <th>Proposal</th>
                    <th className="small-text">Roles (shards)</th>
                    <th>Stake</th>
                    <th>Blocks</th>
                    <th>Produced Chunks</th>
                    <th>Endorsed Chunks</th>
                    {epochData?.status_response.EpochInfo.slice(2).map((epoch: any) => (
                        <th key={epoch.epoch_id} className="small-text">
                            {epoch.epoch_id.substring(0, 4)}...
                        </th>
                    ))}
                </tr>
            </thead>
        );
    }

    function renderTableBody(
        validators: Validators | null,
        maxStake: number,
        totalStake: number,
        maxExpectedBlocks: number,
        maxExpectedChunks: number,
        maxExpectedEndorsements: number,
    ): JSX.Element {
        return (
            <tbody>
                {validators?.sorted().map((validator: ValidatorInfo) => {
                    return (
                        <tr key={validator.accountId}>
                            <td>{validator.accountId}</td>
                            <td>{renderRoles(validator.roles[0], validator.roles[1], validator.kickoutReason, true)}</td>
                            <td>
                            {drawStakeBar(validator.next?.stake ?? null, maxStake, totalStake)}
                        </td>
                        <td>{drawStakeBar(validator.proposalStake, maxStake, totalStake)}</td>
                        <td>{renderRoles(validator.roles[1], validator.roles[2] ?? validator.roles[1], validator.kickoutReason)}</td>
                        <td>
                            {drawStakeBar(
                                validator.current?.stake ?? null,
                                maxStake,
                                totalStake
                            )}
                        </td>
                        <td>
                            {drawProducedAndExpectedBar(
                                validator.current?.blocks ?? null,
                                maxExpectedBlocks
                            )}
                        </td>
                        <td>
                            {drawProducedAndExpectedBar(
                                validator.current?.chunks ?? null,
                                maxExpectedChunks
                            )}
                        </td>
                        <td>
                            {drawProducedAndExpectedBar(
                                validator.current?.endorsements ?? null,
                                maxExpectedEndorsements
                            )}
                        </td>
                        {validator.roles.slice(2).map((roles: ValidatorRole[], i: number) => (
                            <td key={i}>{renderRoles(roles, validator.roles[3 + i] ?? roles, validator.kickoutReason)}</td>
                        ))}
                        </tr>
                    );
                })}
            </tbody>
        );
    }

    function renderValidatorsTable(
        epochData: any,
        validators: Validators | null,
        maxStake: number,
        totalStake: number,
        maxExpectedBlocks: number,
        maxExpectedChunks: number,
        maxExpectedEndorsements: number,
    ): JSX.Element {
        return (
            <table className="epoch-validators-table">
                {renderTableHeaders(epochData)}
                {renderTableBody(
                    validators,
                    maxStake,
                    totalStake,
                    maxExpectedBlocks,
                    maxExpectedChunks,
                    maxExpectedEndorsements
                )}
            </table>
        );
    }

    return (
        <div className={isFetching ? 'loading-overlay' : ''}>
            <div className="kickout-disclaimer">
                Note: Validator kickouts are determined at the end of the <b>second previous epoch</b>
            </div>

            <div className="epoch-navigation">
                <button 
                    onClick={() => handleNavigateEpoch('left')}
                    disabled={!epochData}
                    className="arrow-button"
                >
                    ←
                </button>
                
                <div className="epoch-info">
                    {epochData && (
                        <>
                            <div className="epoch-height">
                                Epoch {epochData.status_response.EpochInfo[1].epoch_height}
                                <span className="epoch-start-height">
                                    (starts at {epochData.status_response.EpochInfo[1].height || 'N/A'})
                                </span>
                            </div>
                            <details>
                                <summary>Recent Epochs</summary>
                                <div className="epoch-debug">
                                    {epochData.status_response.EpochInfo.map((epoch: EpochInfoView, index: number) => (
                                        <div key={epoch.epoch_id}>
                                            Epoch {epoch.epoch_height}: {epoch.epoch_id}
                                        </div>
                                    ))}
                                </div>
                            </details>
                        </>
                    )}
                </div>

                <button 
                    onClick={() => handleNavigateEpoch('right')}
                    disabled={!epochData}
                    className="arrow-button"
                >
                    →
                </button>
            </div>

            <input
                type="text"
                placeholder="Enter Epoch ID"
                value={enteredEpochId}
                onChange={handleEpochIdInput}
            />
            <button onClick={handleButtonClick}>Show Validators</button>
            {renderValidatorsTable(
                epochData,
                validators,
                maxStake,
                totalStake,
                maxExpectedBlocks,
                maxExpectedChunks,
                maxExpectedEndorsements
            )}
        </div>
    );
};

function drawProducedAndExpectedBar(
    producedAndExpected: ProducedAndExpected | null,
    maxExpected: number,
    scale = 1
): JSX.Element {
    if (producedAndExpected === null) {
        return <></>;
    }
    const { produced, expected } = producedAndExpected;
    if (expected == 0) {
        return <div className="expects-zero">0</div>;
    }
    const expectedWidth = (expected / maxExpected) * 100 + 10;
    let producedWidth = (expectedWidth * produced) / expected;
    let missedWidth = (expectedWidth * (expected - produced)) / expected;
    if (produced !== expected) {
        if (producedWidth < 5) {
            producedWidth = 5;
            missedWidth = expectedWidth - producedWidth;
        }
        if (missedWidth < 5) {
            missedWidth = 5;
            producedWidth = expectedWidth - missedWidth;
        }
    }
    return (
        <div className="produced-and-expected-bar">
            <div className="produced-count">{produced}</div>
            <div className="produced" style={{ width: producedWidth * scale }}></div>
            {produced !== expected && (
                <>
                    <div className="missed" style={{ width: missedWidth * scale }}></div>
                    <div className="missed-count">{expected - produced}</div>
                </>
            )}
        </div>
    );
}

function drawStakeBar(stake: number | null, maxStake: number, totalStake: number): JSX.Element {
    if (stake === null) {
        return <></>;
    }
    const width = (stake / maxStake) * 100 + 5;
    const stakeText = Math.floor(stake / 1e24).toLocaleString('en-US');
    const stakePercentage = ((100 * stake) / totalStake).toFixed(2) + '%';
    return (
        <div className="stake-bar">
            <div className="bar" style={{ width }}></div>
            <div className="text">
                {stakeText} ({stakePercentage})
            </div>
        </div>
    );
}

function renderRoles(roles: ValidatorRole[], prev_roles: ValidatorRole[], kickoutReason: ValidatorKickoutReason | null = null, isNextEpoch: boolean = false): JSX.Element {
    if (isNextEpoch && kickoutReason) {
        return <span className="kickout">✖ <KickoutReason reason={kickoutReason} /></span>;
    }

    const renderedItems = [];
    for (const role of roles) {
        switch (role.kind) {
            case 'BlockProducer':
                renderedItems.push(<span className="block-producer">BP</span>);
                break;
            case 'ChunkProducer':
                const [old_shards, new_shards] =
                    partition(role.shards, (shard) => prev_roles?.some(prev_role => prev_role.kind === 'ChunkProducer' && prev_role.shards.includes(shard)));
                // Create a list of shard elements with commas between them
                const shardElements = [
                    ...old_shards.map(shard => <text className="old-chunk-producer">{shard}</text>),
                    ...new_shards.map(shard => <text className="new-chunk-producer">{shard}</text>)
                ].reduce((acc, element, index) => {
                    if (index === 0) {
                        return [element];
                    }
                    return [...acc, <>,</>, element];
                }, [] as JSX.Element[]);
                
                renderedItems.push(
                    <span className="chunk-producer">CP({shardElements})</span>
                );
                break;
            case 'ChunkValidator':
                renderedItems.push(<span className="chunk-validator">CV</span>);
                break;
        }
    }
    
    return <>{renderedItems}</>;
}

const KickoutReason = ({ reason }: { reason: ValidatorKickoutReason | null }) => {
    const id = useId();
    if (reason === null) {
        return <></>;
    }
    let kickoutSummary = '';
    let kickoutReason = '';
    if (reason == 'Slashed') {
        kickoutSummary = 'Slashed';
        kickoutReason = 'Validator was slashed';
    } else if (reason == 'Unstaked') {
        kickoutSummary = 'Unstaked';
        kickoutReason = 'Validator unstaked';
    } else if (reason == 'DidNotGetASeat') {
        kickoutSummary = 'Seat';
        kickoutReason = 'Validator did not get a seat';
    } else if ('NotEnoughBlocks' in reason) {
        kickoutSummary = '#Blocks';
        kickoutReason = `Validator did not produce enough blocks: expected ${reason.NotEnoughBlocks.expected}, actually produced ${reason.NotEnoughBlocks.produced}`;
    } else if ('NotEnoughChunks' in reason) {
        kickoutSummary = '#Chunks';
        kickoutReason = `Validator did not produce enough chunks: expected ${reason.NotEnoughChunks.expected}, actually produced ${reason.NotEnoughChunks.produced}`;
    } else if ('NotEnoughChunkEndorsements' in reason) {
        {/* cspell: words Endors */}
        kickoutSummary = '#Endors';
        kickoutReason = `Validator did not produce enough chunk endorsements: expected ${reason.NotEnoughChunkEndorsements.expected}, actually produced ${reason.NotEnoughChunkEndorsements.produced}`;
    } else if ('NotEnoughStake' in reason) {
        kickoutSummary = 'LowStake';
        kickoutReason = `Validator did not have enough stake: minimum stake required was ${reason.NotEnoughStake.threshold}, but validator only had ${reason.NotEnoughStake.stake}`;
    } else {
        kickoutSummary = 'Other';
        kickoutReason = JSON.stringify(reason);
    }
    return (
        <>
            <span className="kickout-reason" id={id}>
                {kickoutSummary}
            </span>
            <Tooltip anchorId={id} content={kickoutReason} />
        </>
    );
};
