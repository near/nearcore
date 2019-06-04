use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Resource {
    Battery,
    RgbSensor,
    ThermalSensor,
    PoseEstimation,
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Reward {
    Score,
    Token,
    Prediction,
    Currency,
    Policy,
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum Asset {
    Resource(Resource),
    Reward(Reward),
    MissionTime,
    Trust,
}

#[derive(PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    MissionTimeWithResource,
    MissionTimeWithTrust,
}
