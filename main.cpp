#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <pthread.h>
#include <algorithm>

using namespace std;

// structure to store information about each mapper thread
struct MapperThreadArgs
{
    int number_of_input_files;
    map<int, string> *input_files_names;
    int *current_input_file_id; // pointer to the current file id, for dynamic reading of the input files
    map<char, set<pair<string, int>>> *map_data;
    pthread_mutex_t *file_reading_mutex; // pointer to the file reading mutex
    pthread_mutex_t *map_writing_mutex;  // pointer to the map writing mutex
    pthread_barrier_t *barrier;          // pointer to the barrier
};

struct ReducerThreadArgs
{
    map<char, set<pair<string, int>>> *map_data;
    char *current_letter;               // pointer to the current letter, for dynamic reading of the map data
    pthread_mutex_t *map_reading_mutex; // pointer to the map writing mutex
    pthread_barrier_t *barrier;         // pointer to the barrier
};

// function to read the input files
void read_input_files(const string &input_big_file_name, map<int, string> &input_files_names)
{
    // open the input file
    ifstream input_file(input_big_file_name);
    if (!input_file.is_open())
        return;

    int number_of_input_files = 0;
    string line;
    // read the number of input files
    if (getline(input_file, line))
        number_of_input_files = stoi(line);
    if (number_of_input_files <= 0)
        return;
    // read the names of the input files
    for (int i = 0; i < number_of_input_files; i++)
    {
        getline(input_file, line);
        input_files_names.insert({i + 1, line});
    }

    input_file.close();
}

void *mapper_function(void *args)
{
    MapperThreadArgs *mapper_thread_args = (MapperThreadArgs *)args;
    // while there are still files to be processed
    while (true)
    {
        // synchronize access to the current file id
        pthread_mutex_lock(mapper_thread_args->file_reading_mutex);
        // access the current file id and check if all files have been processed
        if (*(mapper_thread_args->current_input_file_id) >= mapper_thread_args->number_of_input_files)
        {
            pthread_mutex_unlock(mapper_thread_args->file_reading_mutex);
            break;
        }
        (*(mapper_thread_args->current_input_file_id))++;
        int current_input_file_id = *(mapper_thread_args->current_input_file_id);
        pthread_mutex_unlock(mapper_thread_args->file_reading_mutex);

        const string &input_file_name = (*mapper_thread_args->input_files_names)[current_input_file_id];
        // open current file for reading
        ifstream input_file(input_file_name);
        if (!input_file.is_open())
            continue;
        // read the current file word by word and store the words in the map
        unordered_map<char, unordered_map<string, set<int>>> partial_map_data;
        string word;
        while (input_file >> word)
        {
            string processed_word;
            // process word by converting it to lowercase characters and removing non-alphabetic characters
            for (char c : word)
            {
                if (c >= 'A' && c <= 'Z')
                    processed_word += c - 'A' + 'a';
                else if (c >= 'a' && c <= 'z')
                    processed_word += c;
            }
            if (processed_word.empty())
                continue;
            char first_letter = processed_word[0];
            // insert the word and the current file id into the map
            partial_map_data[first_letter][processed_word].insert(current_input_file_id);
        }
        input_file.close();
        // synchronize access to the map data
        pthread_mutex_lock(mapper_thread_args->map_writing_mutex);
        // merge the partial map data into the full map data
        for (auto &letter_data : partial_map_data)
        {
            char first_letter = letter_data.first;
            for (auto &word_data : letter_data.second)
            {
                const string &word = word_data.first;
                const set<int> &file_ids = word_data.second;
                for (int file_id : file_ids)
                {
                    (*mapper_thread_args->map_data)[first_letter].insert({word, file_id});
                }
            }
        }
        pthread_mutex_unlock(mapper_thread_args->map_writing_mutex);
    }

    pthread_barrier_wait(mapper_thread_args->barrier);
    return NULL;
}

void *reducer_function(void *args)
{
    ReducerThreadArgs *reducer_thread_args = (ReducerThreadArgs *)args;
    pthread_barrier_wait(reducer_thread_args->barrier);
    // while there are still letters to be processed
    while (true)
    {
        // synchronize access to the current letter
        pthread_mutex_lock(reducer_thread_args->map_reading_mutex);
        // access the current letter and check if all letters have been processed
        if (*(reducer_thread_args->current_letter) >= 'z')
        {
            pthread_mutex_unlock(reducer_thread_args->map_reading_mutex);
            break;
        }
        (*(reducer_thread_args->current_letter))++;
        char current_letter = *(reducer_thread_args->current_letter);
        pthread_mutex_unlock(reducer_thread_args->map_reading_mutex);

        auto &current_letter_word_pairs = (*reducer_thread_args->map_data)[current_letter];
        // create the aggregate list of words and file ids grouped by first letter
        map<string, set<int>> map_words;
        for (const auto &word_pair : current_letter_word_pairs)
        {
            const string &word = word_pair.first;
            const int &file_id = word_pair.second;
            map_words[word].insert(file_id);
        }
        // add map pairs to a vector for sorting
        vector<pair<string, set<int>>> sorted_map_words(map_words.begin(), map_words.end());
        sort(sorted_map_words.begin(), sorted_map_words.end(), [](const auto &a, const auto &b)
             {
            // sort by number of file ids in descending order
            if (a.second.size() != b.second.size())
                return a.second.size() > b.second.size();
            // if number of file ids is equal, sort by word in ascending order
            return a.first < b.first; });
        // create the output files and write the corresponding words and their ids
        string output_file_name = string(1, current_letter) + ".txt";
        ofstream output_file(output_file_name);
        // iterate through map pairs and write to the output file
        for (const auto &pair : sorted_map_words)
        {
            output_file << pair.first << ":[";
            for (auto id = pair.second.begin(); id != pair.second.end(); id++)
            {
                if (id != pair.second.begin())
                    output_file << " ";
                output_file << *id;
            }
            output_file << "]\n";
        }
        output_file.close();
    }
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 4)
        return 1;
    int number_of_mapper_threads = stoi(argv[1]);
    int number_of_reducer_threads = stoi(argv[2]);
    string input_big_file_name = argv[3];

    // read the input files
    map<int, string> input_files_names;
    read_input_files(input_big_file_name, input_files_names);
    if (input_files_names.empty())
        return -1;

    int current_file_id = 0;
    map<char, set<pair<string, int>>> map_data;
    // create and initialize mapper mutexes and barrier
    pthread_mutex_t file_reading_mutex;
    pthread_mutex_init(&file_reading_mutex, NULL);
    pthread_mutex_t map_writing_mutex;
    pthread_mutex_init(&map_writing_mutex, NULL);
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, number_of_mapper_threads + number_of_reducer_threads);
    // initialize mapper thread arguments
    vector<MapperThreadArgs> mapper_thread_args(number_of_mapper_threads);
    for (int i = 0; i < number_of_mapper_threads; i++)
    {
        mapper_thread_args[i].number_of_input_files = input_files_names.size();
        mapper_thread_args[i].input_files_names = &input_files_names;
        mapper_thread_args[i].current_input_file_id = &current_file_id;
        mapper_thread_args[i].map_data = &map_data;
        mapper_thread_args[i].file_reading_mutex = &file_reading_mutex;
        mapper_thread_args[i].map_writing_mutex = &map_writing_mutex;
        mapper_thread_args[i].barrier = &barrier;
    }

    // create and initialize reducer mutex
    char current_letter = 'a' - 1;
    pthread_mutex_t map_reading_mutex;
    pthread_mutex_init(&map_reading_mutex, NULL);
    // initialize reducer thread arguments
    vector<ReducerThreadArgs> reducer_thread_args(number_of_reducer_threads);
    for (int i = 0; i < number_of_reducer_threads; i++)
    {
        reducer_thread_args[i].map_data = &map_data;
        reducer_thread_args[i].current_letter = &current_letter;
        reducer_thread_args[i].map_reading_mutex = &map_reading_mutex;
        reducer_thread_args[i].barrier = &barrier;
    }

    // create and initialize mapper and reducer threads
    vector<pthread_t> mapper_threads(number_of_mapper_threads);
    vector<pthread_t> reducer_threads(number_of_reducer_threads);
    for (int i = 0; i < number_of_mapper_threads + number_of_reducer_threads; i++)
        if (i < number_of_mapper_threads)
            pthread_create(&mapper_threads[i], NULL, mapper_function, &mapper_thread_args[i]);
        else
            pthread_create(&reducer_threads[i - number_of_mapper_threads], NULL, reducer_function, &reducer_thread_args[i - number_of_mapper_threads]);
    for (int i = 0; i < number_of_mapper_threads + number_of_reducer_threads; i++)
        if (i < number_of_mapper_threads)
            pthread_join(mapper_threads[i], NULL);
        else
            pthread_join(reducer_threads[i - number_of_mapper_threads], NULL);

    pthread_mutex_destroy(&file_reading_mutex);
    pthread_mutex_destroy(&map_writing_mutex);
    pthread_mutex_destroy(&map_reading_mutex);
    pthread_barrier_destroy(&barrier);

    return 0;
}
