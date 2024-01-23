def fmt_amount(num):
    return '{:,.1f}'.format(num)

def fmt_comma(num):
    return '{:,}'.format(num)

def fmt_ratio_to_per(num):
    return '{:,.1%}'.format(num)

def fmt_per(num):
    return '{:,.1f}%'.format(num)

def fmt_per2(num):
    return '{:,}%'.format(round(num))
