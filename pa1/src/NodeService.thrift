service NodeService {
  bool ping(),
  bool AddMapTask(1: string filename, 2: string mid_filename),
  bool AddSortTask(1: list<string> mid_file_list, 2: string res_filename, 3: string orig_dir)
}