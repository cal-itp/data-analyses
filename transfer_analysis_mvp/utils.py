def color_waits(x):
    # print(x, end='')
    split = x.split(":")
    mins = abs(int(split[0]))
    secs = int(split[1])
    total_secs = mins * 60 + secs
    total_secs = -total_secs if x[0] == "-" else total_secs
    # print(f'...{total_secs}')
    if total_secs < 0:
        color = "red"
    elif total_secs < 30:
        color = "yellow"
    elif total_secs < 360:
        color = "green"
    elif total_secs < 600:
        color = "yellow"
    elif total_secs < 1200:
        color = "orange"
    return f"background-color: {color};"
