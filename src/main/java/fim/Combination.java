package fim;

import java.util.*;

class Combination {
    /** Function to print all distinct combinations of length `k` from the input
     *  Reference: https://www.geeksforgeeks.org/print-all-possible-combinations-of-r-elements-in-a-given-array-of-size-n/
     * **/

    public static void Combinations(Integer[] input, int index, int k, Set<List<Integer>> subArrays, List<Integer> leaf)
    {
        // invalid input
        if (input.length == 0 || k > input.length) {
            return;
        }

        // base case: combination size is `k`
        if (k == 0) {
            subArrays.add(new ArrayList<>(leaf));
            return;
        }

        // start from the next index till the last index
        for (int itr = index; itr < input.length; itr++)
        {
            // add current element `A[j]` to the solution and recur for next index
            // `j+1` with one less element `k-1`
            leaf.add(input[itr]);
            Combinations(input, itr+ 1, k - 1, subArrays, leaf);
            leaf.remove(leaf.size() - 1);        // backtrack
        }
    }

    public static Set<List<Integer>> findCombinations(Integer[] input, int k)
    {
        Set<List<Integer>> subArray = new HashSet<>();
        if(input.length == k){             //send the input back if input.size == k
            List<Integer> list = new ArrayList<>(input.length);

            for (int i: input) {
                list.add(Integer.valueOf(i));
            }
            subArray.add(list);

        } else {
            Combinations(input, 0, k, subArray, new ArrayList<>());  //calculate all the possible combination
        }
        return subArray;
    }
}