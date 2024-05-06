from flask import Flask, render_template

app = Flask(__name__)

# Route to display the over speeding statistics
@app.route('/')
def display_statistics():
    statistics = parse_statistics('over_speeding_statistics.txt')
    return render_template('statistics.html', statistics=statistics)

# Function to parse the over speeding statistics from the file
def parse_statistics(file_path):
    statistics = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(', ')
            route = parts[0].split(': ')[1]
            truck_id = parts[1].split(': ')[1]
            over_speeding_cases = parts[2].split(': ')[1]
            driver_names = parts[3].split(': ')[1]
            statistics.append({'route': route, 'truck_id': truck_id, 'over_speeding_cases': over_speeding_cases, 'driver_names': driver_names})
    return statistics

if __name__ == '__main__':
    app.run(debug=True)
