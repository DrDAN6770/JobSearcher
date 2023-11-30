from processedfile import processedfile_main

def checknewdata() -> list:
    NeedToDo = processedfile_main()
    if NeedToDo:
        print(f'{"=" * 50}There are new data!{"=" * 50}')
        return NeedToDo
    else:
        print(f'{"=" * 50}No new data!{"=" * 50}')
