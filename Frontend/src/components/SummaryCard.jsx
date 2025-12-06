import React from 'react';
import './SummaryCard.css';

const SummaryCard = ({ icon, label, value, variant = 'primary' }) => {
    return (
        <div className={`summary-card summary-card-${variant} animate-scaleIn`}>
            <div className="summary-card-icon">{icon}</div>
            <div className="summary-card-content">
                <div className="summary-card-label">{label}</div>
                <div className="summary-card-value">{value}</div>
            </div>
        </div>
    );
};

export default SummaryCard;
