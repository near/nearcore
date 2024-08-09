import React, { ChangeEvent, useState } from 'react';

interface LogFileDropProps {
    onFileDrop: (lines: string[]) => void;
}

// Renders a box to drop a file into, and an alternative button to upload.
// onFileDrop is called when a file is dropped or uploaded, with the
// contents of the file as an array of lines.
export const LogFileDrop = ({ onFileDrop }: LogFileDropProps) => {
    const [drag, setDrag] = useState(false);
    const [segments, setSegments] = useState<string[][] | null>(null);

    const handleDrag = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
    };

    const handleDragIn = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(true);
    };

    const handleDragOut = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(false);
    };

    const handleDrop = (e: React.DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDrag(false);
        if (e.dataTransfer?.files.length > 0) {
            handleFile(e.dataTransfer.files[0]);
        }
    };

    const handleFile = (file: File) => {
        const reader = new FileReader();
        reader.onload = (e) => {
            const lines = (e.target!.result as string).split('\n');
            const segments = [];
            let currentSegment = [];
            let currentSegmentHasTestLoop = false;
            for (const line of lines) {
                currentSegment.push(line);
                if (line.includes('TEST_LOOP_SHUTDOWN')) {
                    segments.push(currentSegment);
                    currentSegment = [];
                    currentSegmentHasTestLoop = false;
                }
                if (line.includes('TEST_LOOP_INIT')) {
                    currentSegmentHasTestLoop = true;
                }
            }
            if (currentSegmentHasTestLoop) {
                segments.push(currentSegment);
            }
            setSegments(segments);
            if (segments.length == 1) {
                onFileDrop(segments[0]);
            }
        };
        reader.readAsText(file);
    };

    const handleClick = (event: ChangeEvent<HTMLInputElement>) => {
        const input: HTMLInputElement | null = event.currentTarget;
        if (input && input.files) {
            handleFile(input.files[0]);
        }
    };
    return (
        <div>
            <div
                onDrop={handleDrop}
                onDragEnter={handleDragIn}
                onDragLeave={handleDragOut}
                onDragOver={handleDrag}
                style={{
                    padding: '1rem',
                    border: drag ? '1px solid blue' : '1px solid gray',
                }}>
                Drag and drop a file here
            </div>
            <input type="file" onChange={handleClick} />
            {segments !== null && segments.length > 0 && <div>
                <h3>There are multiple TestLoop runs within the same test. Choose one:</h3>
                {segments.map((segment, i) => (
                    <div key={i}>
                        <button onClick={() => onFileDrop(segment)}>Run #{i + 1}</button>
                    </div>
                ))}
            </div>}
            {segments !== null && segments.length === 0 && <div>The log file does not contain any TestLoop runs.</div>}
        </div>
    );
};
