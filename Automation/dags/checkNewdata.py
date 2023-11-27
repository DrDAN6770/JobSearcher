from processedfile import processedfile_main

def checknewdata() -> list:
    NeedToDo = processedfile_main()
    if NeedToDo:
        print(f'{"==" * 30}There are new data!{"==" * 30}')
        return NeedToDo
    else:
        print(f'{"==" * 30}No new data!{"==" * 30}')

checknewdata()