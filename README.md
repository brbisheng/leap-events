# Career and Leadership Event Aggregator

## Project Description

This project finds events on Eventbrite about career planning, leadership, job fairs, and professional development in Southern California happening in the next 30 days and saves them in two files: `leap_events_socal.csv` and `leap_events_socal.md`.

## Dependencies and Installation Instructions

To use this project you need Python 3; then create a folder for the project, set up a virtual environment (`python -m venv venv` and activate it), install the required packages with `pip install requests beautifulsoup4 pandas pytz`, and set your Eventbrite API token in your environment (`export EVENTBRITE_TOKEN=YOUR_TOKEN_HERE` on macOS/Linux or `set EVENTBRITE_TOKEN=YOUR_TOKEN_HERE` on Windows).

## How to Run the Notebook

Start Jupyter Notebook in the project folder by running `jupyter notebook leap_event.ipynb`, then in your web browser click the Run button on each cell from top to bottom; when it finishes, you will have two new files: `leap_events_socal.csv` and `leap_events_socal.md`.

## Sample Output Preview

Below is an example of one event entry in `leap_events_socal.md`:

```markdown
### Sample Event Title
- **Organizer**: Sample Organizer
- **When (PT)**: 2025-06-10 14:00
- **Where**: Some Venue, Los Angeles
- **Fee**: Free
- **URL**: [Link to event](https://example.com)
- **Description**: A short summary of the event...
```
