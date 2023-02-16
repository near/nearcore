export function toHumanTime(seconds: number): string {
    let result = "";
    if (seconds >= 60) {
        let minutes = Math.floor(seconds / 60);
        seconds = seconds % 60;
        if (minutes > 60) {
            let hours = Math.floor(minutes / 60);
            minutes = minutes % 60;
            if (hours > 24) {
                let days = Math.floor(hours / 24);
                hours = hours % 24;
                result += days + " days ";
            }
            result += hours + " h ";
        }
        result += minutes + " m ";
    }
    result += seconds + " s"
    return result;
}
