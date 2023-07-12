const target = 405;
const nums = [1, 2, 6, 9, 10, 100];

const op = [['+', (a, b) => a + b], ['-', (a, b) => a - b], ['*', (a, b) => a * b], ['/', (a, b) => a / b]];

let numSolutions = 0;
let bestSolution;

function solve(nums, solution) {
  for (let i = 0; i < nums.length; i++) {
    for (let j = 0; j < nums.length; j++) {
      if (j != i) {
        for (let k = 0; k < op.length; k++) {
          const res = op[k][1](nums[i], nums[j]);
          if (Number.isInteger(res)) {
            const newSolution = [...solution, `${nums[i]} ${op[k][0]} ${nums[j]} = ${res}`];
            if (res == target) {
              numSolutions++;
              if (!bestSolution || newSolution.length < bestSolution.length) {
                bestSolution = newSolution;
                console.log(newSolution);
              }
            } else {
              if (newSolution.length < 5) {
                const newNums = nums.slice();
                newNums.splice(i, 1);
                newNums.splice(j - (i < j ? 1 : 0), 1);
                newNums.push(res);
                solve(newNums, newSolution);
              }
            }
          }
        }
      }
    }
  }
}

solve(nums, []);
console.log(numSolutions);