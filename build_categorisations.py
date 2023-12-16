
def tag_loop(response,poss_tags,tag_list):
    n=0
    l=len(poss_tags)
    while n<l:
        name = poss_tags[n]
        call_category = response[['Call ID',name]].copy()
        call_category = call_category.dropna()
        if name == 'Call categories':
            n=n+1
        else:
            tag_list = _call_category(response,name,tag_list)
            n=n+1
    return tag_list, call_category 
    

def _call_category(response,name,tag_list):
    call_category = response[['Call ID',name]].copy()
    call_category = call_category.dropna()
    l=len(call_category)
    n=0
    while n < l:
        tag_list.append([call_category.iloc[n][0],name])
        n=n+1
    return tag_list

def category_loop(poss_cats,calls_categorised,category_list):
    n=0
    l=len(poss_cats)
    while n<l:
        name = poss_cats[n]
        category_list = _category_tables(name,calls_categorised,category_list)
        n=n+1
    return category_list

def _category_tables(name,calls_categorised,category_list):
        results = calls_categorised[["Call ID", name]]
        results = results.dropna() #removes lines where the category didn't hit
        l=len(results)
        n=0
        while n < l:
             category_list.append([results.iloc[n][0],name])
             n=n+1
        return category_list