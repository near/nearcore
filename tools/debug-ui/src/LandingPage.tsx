import { useState } from 'react';
import { useNavigate } from 'react-router';
import './LandingPage.scss';
import { Link } from 'react-router-dom';

export const LandingPage = () => {
    const [addr, setAddr] = useState('');
    const navigate = useNavigate();
    return (
        <div className="landing-page">
            <h3>Debug UI for nearcore</h3>
            <p>Enter address of target node to debug:</p>
            <form>
                <input
                    type="text"
                    value={addr}
                    onChange={(e) => setAddr(e.target.value)}
                    size={40}
                    placeholder="ex. 1.2.3.4:3030 (port optional)"
                />
                <button onClick={() => navigate(`/${addr}/last_blocks`)} type="submit">
                    Go
                </button>
            </form>

            <div className="spacer"></div>
            <h3>Additional links</h3>
            <ul>
                <li>
                    <Link to="/logviz">/logviz</Link>: Log visualizer for nearcore TestLoop tests
                </li>
            </ul>
        </div>
    );
};
