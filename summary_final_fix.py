def analyze_account_balances(excel_file_path, output_file_path=None):
    """
    Analyze account balances and find first imbalance date for each Loc_AcctKey
    
    Parameters:
    excel_file_path (str): Path to the Excel file with data_1 and data_2 sheets
    output_file_path (str): Optional path to save results to Excel
    
    Returns:
    pd.DataFrame: Results with balance analysis
    """
    
    print("Reading Excel file...")
    
    # Read both sheets
    try:
        sheet1 = pd.read_excel(excel_file_path, sheet_name='data_1')
        sheet2 = pd.read_excel(excel_file_path, sheet_name='data_2')
        print(f"Sheet 1 rows: {len(sheet1)}, Sheet 2 rows: {len(sheet2)}")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None
    
    # Combine both sheets
    combined_data = pd.concat([sheet1, sheet2], ignore_index=True)
    print(f"Combined data rows: {len(combined_data)}")
    
    # Clean and standardize column names (handle case variations)
    combined_data.columns = combined_data.columns.str.upper().str.strip()
    
    # Map possible column name variations
    column_mapping = {
        'LOC_ACCTKEY': ['LOC_ACCTKEY', 'LOCACCTKEY', 'LOC_ACCT_KEY'],
        'TRANS_AM': ['TRANS_AM', 'TRANS_AMOUNT', 'AMOUNT'],
        'TRANS_DIRCTN_MN': ['TRANS_DIRCTN_MN', 'TRANS_DIRECTION', 'DIRECTION'],
        'POST_DT': ['POST_DT', 'POST_DATE', 'POSTING_DATE', 'DATE'],
        'CHQ_ACCT_NO': ['CHQ_ACCT_NO', 'CHQ_ACCOUNT_NO', 'ACCOUNT_NO', 'ACCT_NO'],
        'CHQ_STATUS': ['CHQ_STATUS', 'ACCOUNT_STATUS', 'STATUS', 'ACCT_STATUS']
    }
    
    # Find actual column names in the data
    actual_columns = {}
    for standard_name, possible_names in column_mapping.items():
        found = False
        for possible_name in possible_names:
            if possible_name in combined_data.columns:
                actual_columns[standard_name] = possible_name
                found = True
                break
        if not found and standard_name not in ['CHQ_ACCT_NO', 'CHQ_STATUS']:  # CHQ_ACCT_NO and CHQ_STATUS are optional
            print(f"Warning: Could not find column for {standard_name}")
            print(f"Available columns: {list(combined_data.columns)}")
    
    print(f"Column mapping: {actual_columns}")
    
    # Rename columns to standard names
    combined_data = combined_data.rename(columns={v: k for k, v in actual_columns.items()})
    
    # Verify required columns exist
    required_cols = ['LOC_ACCTKEY', 'TRANS_AM', 'TRANS_DIRCTN_MN', 'POST_DT']
    missing_cols = [col for col in required_cols if col not in combined_data.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        return None
    
    # Clean the data
    print("Cleaning data...")
    
    # Convert POST_DT to datetime and normalize (remove time component)
    combined_data['POST_DT'] = pd.to_datetime(combined_data['POST_DT'], errors='coerce').dt.normalize()
    
    # Convert TRANS_AM to numeric
    combined_data['TRANS_AM'] = pd.to_numeric(combined_data['TRANS_AM'], errors='coerce')
    
    # Clean transaction direction
    combined_data['TRANS_DIRCTN_MN'] = combined_data['TRANS_DIRCTN_MN'].astype(str).str.upper().str.strip()
    
    # Remove rows with missing critical data
    initial_rows = len(combined_data)
    combined_data = combined_data.dropna(subset=['LOC_ACCTKEY', 'TRANS_AM', 'TRANS_DIRCTN_MN', 'POST_DT'])
    print(f"Removed {initial_rows - len(combined_data)} rows with missing data")
    
    # Filter for CR and DR transactions only
    combined_data = combined_data[combined_data['TRANS_DIRCTN_MN'].isin(['CR', 'DR'])]
    print(f"Final data rows: {len(combined_data)}")
    
    # Sort by account and date
    combined_data = combined_data.sort_values(['LOC_ACCTKEY', 'POST_DT']).reset_index(drop=True)
    
    print("Analyzing account balances...")
    
    # First, identify accounts with total imbalances
    print("Step 1: Checking total account imbalances...")
    account_totals = combined_data.groupby(['LOC_ACCTKEY', 'TRANS_DIRCTN_MN'])['TRANS_AM'].sum().unstack(fill_value=0)
    account_totals['total_imbalance'] = account_totals.get('CR', 0) - account_totals.get('DR', 0)
    account_totals['is_totally_balanced'] = abs(account_totals['total_imbalance']) < 1e-10
    
    imbalanced_accounts = account_totals[~account_totals['is_totally_balanced']].index.tolist()
    balanced_accounts = account_totals[account_totals['is_totally_balanced']].index.tolist()
    
    print(f"Total accounts: {len(account_totals)}")
    print(f"Totally balanced accounts: {len(balanced_accounts)}")
    print(f"Imbalanced accounts to analyze: {len(imbalanced_accounts)}")
    
    # Group by account and calculate running balances (focus on imbalanced accounts)
    results = []
    
    # Process all accounts but focus analysis on imbalanced ones
    for account in combined_data['LOC_ACCTKEY'].unique():
        account_data = combined_data[combined_data['LOC_ACCTKEY'] == account].copy()
        
        # Add month_year column
        account_data['month_year'] = account_data['POST_DT'].dt.to_period('M')
        
        # Calculate monthly aggregations
        monthly_cr = account_data[account_data['TRANS_DIRCTN_MN'] == 'CR'].groupby('month_year')['TRANS_AM'].sum()
        monthly_dr = account_data[account_data['TRANS_DIRCTN_MN'] == 'DR'].groupby('month_year')['TRANS_AM'].sum()
        
        # Get all unique dates for this account
        all_dates = sorted(account_data['POST_DT'].unique())
        
        # Initialize tracking variables
        running_cr = 0
        running_dr = 0
        first_imbalance_date = None
        first_imbalance_found = False
        previous_imbalance = 0
        
        # Only do detailed line-by-line analysis for imbalanced accounts
        if account in imbalanced_accounts:
            print(f"Analyzing imbalanced account: {account}")
            
            for post_date in all_dates:
                post_date = pd.Timestamp(post_date)
                
                # Get transactions for this date
                daily_data = account_data[account_data['POST_DT'] == post_date]
                daily_cr_amount = daily_data[daily_data['TRANS_DIRCTN_MN'] == 'CR']['TRANS_AM'].sum()
                daily_dr_amount = daily_data[daily_data['TRANS_DIRCTN_MN'] == 'DR']['TRANS_AM'].sum()
                
                # Update running totals
                running_cr += daily_cr_amount
                running_dr += daily_dr_amount
                
                # Calculate current running imbalance
                current_imbalance = running_cr - running_dr
                
                # Handle first imbalance and re-balancing logic based on running totals
                running_totals_balanced = abs(running_cr - running_dr) < 1e-10
                
                if not running_totals_balanced and not first_imbalance_found:
                    # First time becoming imbalanced in running totals
                    first_imbalance_date = post_date
                    first_imbalance_found = True
                elif running_totals_balanced and first_imbalance_found:
                    # Account became balanced again in running totals - reset the first_imbalance_date
                    first_imbalance_date = None
                    first_imbalance_found = False
                elif not running_totals_balanced and first_imbalance_found and first_imbalance_date is None:
                    # Account became imbalanced again after being balanced in running totals
                    first_imbalance_date = post_date
                
                # Calculate imbalance difference (change from previous date)
                imbalance_difference = current_imbalance - previous_imbalance
                
                # Get month_year for this date
                month_year = post_date.to_period('M')
                
                # Add to results
                results.append({
                    'LOC_ACCTKEY': account,
                    'post_date': post_date,
                    'month_year': str(month_year),
                    'monthly_cr': daily_cr_amount,  # Daily CR amount for this post_date
                    'monthly_dr': daily_dr_amount,  # Daily DR amount for this post_date
                    'running_cr_total': running_cr,
                    'running_dr_total': running_dr,
                    'current_imbalance': current_imbalance,
                    'imbalance_difference': imbalance_difference,
                    'is_balanced': running_totals_balanced,
                    'first_imbalance_date': first_imbalance_date,
                    'account_totally_balanced': False
                })
                
                # Update previous imbalance for next iteration
                previous_imbalance = current_imbalance
        
        else:
            # For totally balanced accounts, just add summary record
            total_cr = account_totals.loc[account, 'CR'] if 'CR' in account_totals.columns else 0
            total_dr = account_totals.loc[account, 'DR'] if 'DR' in account_totals.columns else 0
            
            results.append({
                'LOC_ACCTKEY': account,
                'post_date': all_dates[-1] if all_dates else None,  # Last transaction date
                'month_year': str(pd.Timestamp(all_dates[-1]).to_period('M')) if all_dates else None,
                'monthly_cr': 0,  # No daily transactions for totally balanced accounts
                'monthly_dr': 0,  # No daily transactions for totally balanced accounts
                'running_cr_total': total_cr,
                'running_dr_total': total_dr,
                'current_imbalance': total_cr - total_dr,
                'imbalance_difference': 0,
                'is_balanced': True,
                'first_imbalance_date': None,
                'account_totally_balanced': True
            })
    
    # Convert to DataFrame
    results_df = pd.DataFrame(results)
    
    print(f"Analysis complete. Found {len(results_df)} records for {results_df['LOC_ACCTKEY'].nunique()} accounts.")
    
    # Summary statistics
    imbalanced_accounts_with_dates = results_df[results_df['account_totally_balanced'] == False]['LOC_ACCTKEY'].nunique()
    print(f"Accounts with current imbalances (using account_totally_balanced): {imbalanced_accounts_with_dates}")
    
    if imbalanced_accounts_with_dates > 0:
        earliest_imbalance = results_df[results_df['first_imbalance_date'].notna()]['first_imbalance_date'].min()
        print(f"Earliest current imbalance date: {earliest_imbalance}")
    
    # Save to Excel if output path provided
    if output_file_path:
        try:
            with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                results_df.to_excel(writer, sheet_name='Balance_Analysis', index=False)
                
                # Create comprehensive summary sheet
                summary_data = []
                for account in results_df['LOC_ACCTKEY'].unique():
                    account_results = results_df[results_df['LOC_ACCTKEY'] == account]
                    
                    # Get the final balance status (last record for the account)
                    final_record = account_results.iloc[-1]
                    
                    # Handle imbalance dates more robustly
                    try:
                        # Get ALL imbalance dates (all unique non-null values)
                        imbalance_dates_series = account_results['first_imbalance_date'].dropna()
                        
                        if len(imbalance_dates_series) > 0:
                            # Get unique dates and convert to list
                            all_imbalance_dates = imbalance_dates_series.unique()
                            # Convert each date to string format safely
                            date_strings = []
                            for date in sorted(all_imbalance_dates):
                                if pd.notna(date):
                                    # Convert to string directly
                                    date_str = str(date)[:10]  # Take only YYYY-MM-DD part
                                    date_strings.append(date_str)
                            
                            all_imbalance_dates_str = ', '.join(date_strings) if date_strings else None
                            number_of_imbalance_periods = len(date_strings)
                        else:
                            all_imbalance_dates_str = None
                            number_of_imbalance_periods = 0
                            
                    except Exception as e:
                        error_msg = f"Error processing imbalance dates for account {account}: {e}"
                        print(error_msg)
                        all_imbalance_dates_str = None
                        number_of_imbalance_periods = 0
                        latest_imbalance_date = None
                    
                    # Handle the case where account_results might have different structures
                    try:
                        total_cr = final_record['running_cr_total']
                        total_dr = final_record['running_dr_total']
                        final_imbalance = final_record['current_imbalance']
                        finally_balanced = final_record['is_balanced']
                        
                        # Clean up floating point errors
                        if abs(final_imbalance) < 1e-10:
                            final_imbalance = 0.0
                            finally_balanced = True
                            dr_greater_than_cr = False
                        else:
                            dr_greater_than_cr = total_dr > total_cr
                        
                        # Get latest imbalance date (most recent imbalance)
                        latest_imbalance_date = None
                        if len(imbalance_dates_series) > 0:
                            latest_imbalance_date = max(imbalance_dates_series)
                            latest_imbalance_date = str(latest_imbalance_date)[:10] if pd.notna(latest_imbalance_date) else None
                            
                    except KeyError as e:
                        print(f"Missing column for account {account}: {e}")
                        continue
                    
                    summary_data.append({
                        'LOC_ACCTKEY': account,
                        'total_CR': total_cr,
                        'total_DR': total_dr,
                        'final_imbalance': final_imbalance,
                        'all_imbalance_dates': all_imbalance_dates_str,
                        'latest_imbalance_date': latest_imbalance_date,
                        'number_of_imbalance_periods': number_of_imbalance_periods,
                        'finally_balanced': finally_balanced,
                        'DR > CR': dr_greater_than_cr
                    })
                
                summary_df = pd.DataFrame(summary_data)
                
                # Save the original Account_Summary
                summary_df.to_excel(writer, sheet_name='Account_Summary', index=False)
                
                # Analysis 1: Check imbalanced accounts against closed account status
                print("Creating checked_chq_status analysis...")
                print("Using account_totally_balanced to identify currently imbalanced accounts...")
                imbalanced_accounts_df = results_df[results_df['account_totally_balanced'] == False]['LOC_ACCTKEY'].unique()
                
                # Get summary info for these accounts
                imbalanced_summary = []
                for account in imbalanced_accounts_df:
                    account_summary_row = summary_df[summary_df['LOC_ACCTKEY'] == account].iloc[0]
                    imbalanced_summary.append(account_summary_row)
                imbalanced_accounts_summary_df = pd.DataFrame(imbalanced_summary)
                
                print(f"Found {len(imbalanced_accounts_df)} currently imbalanced accounts for detailed analysis")
                
                checked_status_data = []
                
                # Check if CHQ_STATUS column exists after mapping
                has_chq_status = 'CHQ_STATUS' in combined_data.columns
                if has_chq_status:
                    print("CHQ_STATUS column found - will analyze closed account status")
                else:
                    print("CHQ_STATUS column not found - skipping closed account status analysis")

                for account in imbalanced_accounts_df:
                    account_data = combined_data[combined_data['LOC_ACCTKEY'] == account]
                    
                    # Get account balance info
                    account_summary = imbalanced_accounts_summary_df[imbalanced_accounts_summary_df['LOC_ACCTKEY'] == account].iloc[0]
                    
                    # Initialize default values
                    closed_only_cr = 0
                    closed_only_dr = 0
                    closed_only_imbalance = account_summary['final_imbalance']
                    is_balanced_when_closed_only = False
                    
                    # Initialize new Open-only values
                    open_only_cr = 0
                    open_only_dr = 0
                    open_only_imbalance = 0
                    is_balanced_when_open_only = False
                    is_open_and_close_same = False
                    
                    status_check_applicable = False
                    all_statuses = []
                    
                    if has_chq_status:
                        # Get all unique statuses for this LOC_ACCTKEY
                        all_statuses = account_data['CHQ_STATUS'].dropna().unique().tolist()
                        all_statuses_upper = [str(status).upper().strip() for status in all_statuses]
                        
                        # Check if this account group only has Open and Closed Account statuses
                        valid_statuses = {'OPEN', 'CLOSED ACCOUNT'}
                        has_both_open_and_closed = 'OPEN' in all_statuses_upper and 'CLOSED ACCOUNT' in all_statuses_upper
                        only_open_closed = set(all_statuses_upper).issubset(valid_statuses)
                        
                        if only_open_closed and has_both_open_and_closed:
                            status_check_applicable = True
                            print(f"Account {account} has only Open/Closed Account statuses - analyzing BOTH open and closed account transactions")
                            
                            # Filter for ONLY Closed Account transactions
                            closed_data = account_data[account_data['CHQ_STATUS'].str.upper().str.strip() == 'CLOSED ACCOUNT']
                            
                            if len(closed_data) > 0:
                                # Calculate totals for closed account transactions only
                                closed_only_cr = closed_data[closed_data['TRANS_DIRCTN_MN'] == 'CR']['TRANS_AM'].sum()
                                closed_only_dr = closed_data[closed_data['TRANS_DIRCTN_MN'] == 'DR']['TRANS_AM'].sum()
                                closed_only_imbalance = closed_only_cr - closed_only_dr
                                
                                # Clean up floating point errors
                                if abs(closed_only_imbalance) < 1e-10:
                                    closed_only_imbalance = 0.0
                                    is_balanced_when_closed_only = True
                            
                            # Filter for ONLY Open transactions
                            open_data = account_data[account_data['CHQ_STATUS'].str.upper().str.strip() == 'OPEN']
                            
                            if len(open_data) > 0:
                                # Calculate totals for open transactions only
                                open_only_cr = open_data[open_data['TRANS_DIRCTN_MN'] == 'CR']['TRANS_AM'].sum()
                                open_only_dr = open_data[open_data['TRANS_DIRCTN_MN'] == 'DR']['TRANS_AM'].sum()
                                open_only_imbalance = open_only_cr - open_only_dr
                                
                                # Clean up floating point errors
                                if abs(open_only_imbalance) < 1e-10:
                                    open_only_imbalance = 0.0
                                    is_balanced_when_open_only = True
                            
                            # Check if open and closed imbalances are the same
                            if abs(open_only_imbalance - closed_only_imbalance) < 1e-10:
                                is_open_and_close_same = True
                            
                            print(f"  Original imbalance: {account_summary['final_imbalance']:.2f}")
                            print(f"  Open-only imbalance: {open_only_imbalance:.2f}")
                            print(f"  Closed-only imbalance: {closed_only_imbalance:.2f}")
                            print(f"  Open and Closed same: {is_open_and_close_same}")
                            print(f"  Open transactions: {len(open_data)} records")
                            print(f"  Closed account transactions: {len(closed_data)} records")
                                
                        elif len(all_statuses_upper) > 0:
                            status_list = ', '.join(all_statuses_upper)
                            print(f"Account {account} has statuses: {status_list} - not applicable for Open/Closed Account analysis")
                    
                    # Create status string safely
                    status_string = ', '.join([str(s) for s in all_statuses]) if all_statuses else 'N/A'
                    
                    checked_status_data.append({
                        'LOC_ACCTKEY': account,
                        'original_final_imbalance': account_summary['final_imbalance'],
                        'total_CR': account_summary['total_CR'],
                        'total_DR': account_summary['total_DR'],
                        'all_chq_statuses': status_string,
                        'status_check_applicable': status_check_applicable,
                        'open_only_CR': open_only_cr,
                        'open_only_DR': open_only_dr,
                        'open_only_imbalance': open_only_imbalance,
                        'is_balanced_when_open_only': is_balanced_when_open_only,
                        'closed_only_CR': closed_only_cr,
                        'closed_only_DR': closed_only_dr,
                        'closed_only_imbalance': closed_only_imbalance,
                        'is_balanced_when_closed_only': is_balanced_when_closed_only,
                        'Is_OPEN_and_CLOSE_same': is_open_and_close_same,
                        'adjusted_balance_status': 'Balanced (Closed Only)' if is_balanced_when_closed_only else 'Still Imbalanced',
                        'needs_review': not is_balanced_when_closed_only,
                        'transaction_count': len(account_data)
                    })

                checked_status_df = pd.DataFrame(checked_status_data)
                checked_status_df.to_excel(writer, sheet_name='checked_chq_status', index=False)
                
                # Print summary of closed account analysis
                if has_chq_status:
                    applicable_count = checked_status_df['status_check_applicable'].sum()
                    balanced_when_closed = checked_status_df['is_balanced_when_closed_only'].sum()
                    print(f"Closed account analysis: {applicable_count} accounts applicable, {balanced_when_closed} balanced when considering only closed transactions")

                # Analysis 2: Check multiple CHQ_ACCT_NO for imbalanced accounts
                print("Creating CHQ_ACCT_NO analysis...")
                print("Analyzing CHQ account breakdowns for currently imbalanced accounts...")
                chq_analysis_data = []
                chq_summary_for_account_summary = {}  # Store summary for summary_final tab

                # Check if CHQ_ACCT_NO column exists in the data
                if 'CHQ_ACCT_NO' in combined_data.columns:
                    for account in imbalanced_accounts_df:
                        account_data = combined_data[combined_data['LOC_ACCTKEY'] == account]
                        
                        # Get unique CHQ_ACCT_NO for this LOC_ACCTKEY
                        unique_chq_accounts = account_data['CHQ_ACCT_NO'].dropna().unique()
                        
                        chq_balances = []  # Store balances for this LOC_ACCTKEY
                        
                        if len(unique_chq_accounts) > 1:
                            print(f"Found multiple CHQ_ACCT_NO for {account}: {unique_chq_accounts}")
                            
                            for chq_acct in unique_chq_accounts:
                                chq_data = account_data[account_data['CHQ_ACCT_NO'] == chq_acct]
                                
                                # Calculate totals for this CHQ_ACCT_NO
                                chq_cr_total = chq_data[chq_data['TRANS_DIRCTN_MN'] == 'CR']['TRANS_AM'].sum()
                                chq_dr_total = chq_data[chq_data['TRANS_DIRCTN_MN'] == 'DR']['TRANS_AM'].sum()
                                chq_imbalance = chq_cr_total - chq_dr_total
                                
                                # Clean up floating point errors
                                if abs(chq_imbalance) < 1e-10:
                                    chq_imbalance = 0.0
                                    chq_is_balanced = True
                                else:
                                    chq_is_balanced = False
                                
                                chq_balances.append({
                                    'chq_acct': chq_acct,
                                    'imbalance': chq_imbalance,
                                    'is_balanced': chq_is_balanced
                                })
                                
                                chq_analysis_data.append({
                                    'LOC_ACCTKEY': account,
                                    'CHQ_ACCT_NO': chq_acct,
                                    'CHQ_CR_total': chq_cr_total,
                                    'CHQ_DR_total': chq_dr_total,
                                    'CHQ_imbalance': chq_imbalance,
                                    'CHQ_is_balanced': chq_is_balanced,
                                    'total_CHQ_accounts': len(unique_chq_accounts),
                                    'transaction_count': len(chq_data),
                                    'CHQ_DR_greater_than_CR': chq_dr_total > chq_cr_total if not chq_is_balanced else False
                                })
                            
                            # Determine if any CHQ account is balanced
                            any_balanced = any(bal['is_balanced'] for bal in chq_balances)
                            
                            if any_balanced:
                                # If at least one CHQ is balanced, flag the whole LOC_ACCTKEY as balanced
                                highest_imbalance = 0.0  # Consider it balanced
                                chq_acct_flag = True
                            else:
                                # If all CHQ accounts are imbalanced, use highest imbalance
                                highest_imbalance = max(chq_balances, key=lambda x: abs(x['imbalance']))['imbalance']
                                chq_acct_flag = False
                            
                            # Store summary for summary_final tab
                            chq_summary_for_account_summary[account] = {
                                'chq_acct_balanced': chq_acct_flag,
                                'final_imbalance_amt': highest_imbalance
                            }
                            
                            # Add chq_acct_balanced flag to each row for this account
                            for i, data in enumerate(chq_analysis_data):
                                if data['LOC_ACCTKEY'] == account:
                                    chq_analysis_data[i]['chq_acct_balanced'] = chq_acct_flag
                                    
                        elif len(unique_chq_accounts) == 1:
                            # Single CHQ_ACCT_NO for this LOC_ACCTKEY
                            chq_acct = unique_chq_accounts[0]
                            chq_data = account_data[account_data['CHQ_ACCT_NO'] == chq_acct]
                            chq_cr_total = chq_data[chq_data['TRANS_DIRCTN_MN'] == 'CR']['TRANS_AM'].sum()
                            chq_dr_total = chq_data[chq_data['TRANS_DIRCTN_MN'] == 'DR']['TRANS_AM'].sum()
                            chq_imbalance = chq_cr_total - chq_dr_total
                            
                            # Clean up floating point errors
                            if abs(chq_imbalance) < 1e-10:
                                chq_imbalance = 0.0
                                chq_is_balanced = True
                            else:
                                chq_is_balanced = False
                            
                            # For single CHQ account, use original final_imbalance
                            original_imbalance = imbalanced_accounts_summary_df[imbalanced_accounts_summary_df['LOC_ACCTKEY'] == account]['final_imbalance'].iloc[0]
                            
                            # Store summary for summary_final tab
                            chq_summary_for_account_summary[account] = {
                                'chq_acct_balanced': chq_is_balanced,
                                'final_imbalance_amt': original_imbalance
                            }
                            
                            chq_analysis_data.append({
                                'LOC_ACCTKEY': account,
                                'CHQ_ACCT_NO': chq_acct,
                                'CHQ_CR_total': chq_cr_total,
                                'CHQ_DR_total': chq_dr_total,
                                'CHQ_imbalance': chq_imbalance,
                                'CHQ_is_balanced': chq_is_balanced,
                                'total_CHQ_accounts': 1,
                                'transaction_count': len(chq_data),
                                'CHQ_DR_greater_than_CR': chq_dr_total > chq_cr_total if not chq_is_balanced else False,
                                'chq_acct_balanced': chq_is_balanced
                            })
                        else:
                            # No CHQ_ACCT_NO data for this account
                            original_imbalance = imbalanced_accounts_summary_df[imbalanced_accounts_summary_df['LOC_ACCTKEY'] == account]['final_imbalance'].iloc[0]
                            
                            # Store summary for summary_final tab
                            chq_summary_for_account_summary[account] = {
                                'chq_acct_balanced': False,
                                'final_imbalance_amt': original_imbalance
                            }
                            
                            chq_analysis_data.append({
                                'LOC_ACCTKEY': account,
                                'CHQ_ACCT_NO': 'No CHQ_ACCT_NO',
                                'CHQ_CR_total': 0,
                                'CHQ_DR_total': 0,
                                'CHQ_imbalance': 0,
                                'CHQ_is_balanced': False,
                                'total_CHQ_accounts': 0,
                                'transaction_count': len(account_data),
                                'CHQ_DR_greater_than_CR': False,
                                'chq_acct_balanced': False
                            })

                    chq_analysis_df = pd.DataFrame(chq_analysis_data)
                    if not chq_analysis_df.empty:
                        chq_analysis_df.to_excel(writer, sheet_name='chq_account_analysis', index=False)
                else:
                    print("CHQ_ACCT_NO column not found in data - skipping CHQ account analysis")
                    # Create empty summary for accounts without CHQ_ACCT_NO
                    for account in imbalanced_accounts_df:
                        original_imbalance = imbalanced_accounts_summary_df[imbalanced_accounts_summary_df['LOC_ACCTKEY'] == account]['final_imbalance'].iloc[0]
                        chq_summary_for_account_summary[account] = {
                            'chq_acct_balanced': False,
                            'final_imbalance_amt': original_imbalance
                        }

                # Create the NEW summary_final tab with all analysis results
                print("Creating summary_final tab with complete analysis...")
                
                # Create a copy of the original summary_df for the final summary
                summary_final_df = summary_df.copy()
                
                # Function to get CHQ account balanced status
                def get_chq_acct_balanced(row):
                    # If finally_balanced = True, then chq_acct_balanced should automatically be True
                    if row['finally_balanced']:
                        return True
                    else:
                        return chq_summary_for_account_summary.get(row['LOC_ACCTKEY'], {}).get('chq_acct_balanced', False)
                
                # Function to get final imbalance amount
                def get_final_imbalance_amt(row):
                    if row['finally_balanced']:
                        return 0.0  # If finally balanced, imbalance amount should be 0
                    else:
                        chq_balanced = chq_summary_for_account_summary.get(row['LOC_ACCTKEY'], {}).get('chq_acct_balanced', False)
                        if chq_balanced:
                            return 0.0  # If any CHQ is balanced, consider final imbalance as 0
                        else:
                            return chq_summary_for_account_summary.get(row['LOC_ACCTKEY'], {}).get('final_imbalance_amt', row['final_imbalance'])
                
                # Function to determine if DR > CR based on final imbalance amount
                def get_dr_greater_than_cr_final(row):
                    # Based on final_imbalance_amt sign (negative = DR > CR)
                    final_imbalance_amt = get_final_imbalance_amt(row)
                    return final_imbalance_amt < 0
                
                # Add new columns to summary_final_df
                summary_final_df['chq_acct_balanced'] = summary_final_df.apply(get_chq_acct_balanced, axis=1)
                summary_final_df['final_imbalance_amt'] = summary_final_df.apply(get_final_imbalance_amt, axis=1)
                summary_final_df['DR > CR (Final)'] = summary_final_df.apply(get_dr_greater_than_cr_final, axis=1)
                
                # Add CHQ status analysis results if available
                if has_chq_status and not checked_status_df.empty:
                    # Create a mapping for CHQ status analysis
                    chq_status_mapping = {}
                    for _, row in checked_status_df.iterrows():
                        chq_status_mapping[row['LOC_ACCTKEY']] = {
                            'status_check_applicable': row['status_check_applicable'],
                            'is_balanced_when_closed_only': row['is_balanced_when_closed_only'],
                            'is_balanced_when_open_only': row['is_balanced_when_open_only'],
                            'closed_only_imbalance': row['closed_only_imbalance'],
                            'open_only_imbalance': row['open_only_imbalance']
                        }
                    
                    # Add CHQ status columns
                    summary_final_df['status_check_applicable'] = summary_final_df['LOC_ACCTKEY'].map(
                        lambda x: chq_status_mapping.get(x, {}).get('status_check_applicable', False)
                    )
                    summary_final_df['balanced_when_closed_only'] = summary_final_df['LOC_ACCTKEY'].map(
                        lambda x: chq_status_mapping.get(x, {}).get('is_balanced_when_closed_only', False)
                    )
                    summary_final_df['balanced_when_open_only'] = summary_final_df['LOC_ACCTKEY'].map(
                        lambda x: chq_status_mapping.get(x, {}).get('is_balanced_when_open_only', False)
                    )
                    summary_final_df['closed_only_imbalance'] = summary_final_df['LOC_ACCTKEY'].map(
                        lambda x: chq_status_mapping.get(x, {}).get('closed_only_imbalance', 0.0)
                    )
                    summary_final_df['open_only_imbalance'] = summary_final_df['LOC_ACCTKEY'].map(
                        lambda x: chq_status_mapping.get(x, {}).get('open_only_imbalance', 0.0)
                    )
                
                # Add summary analysis columns
                summary_final_df['needs_further_review'] = ~(
                    summary_final_df['finally_balanced'] | 
                    summary_final_df['chq_acct_balanced'] |
                    (summary_final_df.get('balanced_when_closed_only', False) if has_chq_status else False)
                )
                
                # Determine final resolution status
                def get_resolution_status(row):
                    if row['finally_balanced']:
                        return 'Fully Balanced'
                    elif row['chq_acct_balanced']:
                        return 'Balanced via CHQ Analysis'
                    elif row.get('balanced_when_closed_only', False):
                        return 'Balanced when Closed Only'
                    elif row.get('balanced_when_open_only', False):
                        return 'Balanced when Open Only'
                    else:
                        return 'Still Imbalanced - Needs Review'
                
                summary_final_df['resolution_status'] = summary_final_df.apply(get_resolution_status, axis=1)
                
                # Save the new summary_final tab
                summary_final_df.to_excel(writer, sheet_name='summary_final', index=False)
                
                # Create account totals sheet with CHQ flag
                account_totals_reset = account_totals.reset_index()
                
                # Add chq_acct_flag to account_totals
                account_totals_reset['chq_acct_flag'] = account_totals_reset['LOC_ACCTKEY'].map(
                    lambda x: chq_summary_for_account_summary.get(x, {}).get('chq_acct_balanced', False)
                )
                
                account_totals_reset.to_excel(writer, sheet_name='Account_Totals', index=False)
                
                # Print summary statistics
                print(f"\n=== FINAL SUMMARY STATISTICS ===")
                print(f"Total accounts analyzed: {len(summary_final_df)}")
                print(f"Fully balanced accounts: {(summary_final_df['finally_balanced'] == True).sum()}")
                print(f"Balanced via CHQ analysis: {(summary_final_df['chq_acct_balanced'] == True).sum()}")
                if has_chq_status:
                    print(f"Balanced when closed only: {(summary_final_df.get('balanced_when_closed_only', False) == True).sum()}")
                    print(f"Balanced when open only: {(summary_final_df.get('balanced_when_open_only', False) == True).sum()}")
                print(f"Still need review: {(summary_final_df['needs_further_review'] == True).sum()}")
                
            print(f"Results saved to: {output_file_path}")
            print("New tab 'summary_final' contains complete analysis with all flags and status indicators.")
            
        except Exception as e:
            print(f"Error saving Excel file: {e}")
    
    return results_df